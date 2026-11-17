// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
)

// OpenAICompatible connects to local servers such as LM Studio and llama.cpp.
// The name describes the wire format only; Localzie does not route these
// requests to OpenAI.
type OpenAICompatible struct {
	base   *url.URL
	apiKey string
	client *http.Client
}

func NewOpenAICompatible(rawURL, apiKey string) (*OpenAICompatible, error) {
	base, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("runtime URL must use http or https")
	}
	return &OpenAICompatible{base: base, apiKey: apiKey, client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (o *OpenAICompatible) Name() string { return "openai-compatible" }

func (o *OpenAICompatible) Models(ctx context.Context) ([]protocol.Model, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.endpoint("/v1/models"), nil)
	if err != nil {
		return nil, err
	}
	o.authorize(req)
	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("compatible runtime models", resp)
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	models := make([]protocol.Model, 0, len(result.Data))
	for _, item := range result.Data {
		if item.ID != "" {
			models = append(models, protocol.Model{ID: item.ID, Name: item.ID, Runtime: o.Name(), Ready: true, Locality: "local"})
		}
	}
	return models, nil
}

func (o *OpenAICompatible) Stream(ctx context.Context, request StreamRequest, onDelta func(string) error) error {
	type chatMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	messages := make([]chatMessage, 0, len(request.Messages))
	for _, message := range request.Messages {
		if message.Content != "" {
			messages = append(messages, chatMessage{Role: string(message.Role), Content: message.Content})
		}
	}
	payload := map[string]any{"model": request.Model, "messages": messages, "stream": true}
	if request.MaxOutputTokens > 0 {
		payload["max_tokens"] = request.MaxOutputTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint("/v1/chat/completions"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	o.authorize(req)
	client := *o.client
	client.Timeout = 0
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError("compatible runtime chat", resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	var eventData []string
	done := false
	dispatch := func() error {
		if len(eventData) == 0 {
			return nil
		}
		data := strings.Join(eventData, "\n")
		eventData = eventData[:0]
		if strings.TrimSpace(data) == "[DONE]" {
			done = true
			return nil
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return fmt.Errorf("decode compatible runtime stream: %w", err)
		}
		if chunk.Error != nil {
			return fmt.Errorf("compatible runtime: %s", chunk.Error.Message)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				if err := onDelta(choice.Delta.Content); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			if done {
				return nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			eventData = append(eventData, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := dispatch(); err != nil {
		return err
	}
	if done {
		return nil
	}
	return io.ErrUnexpectedEOF
}

func (o *OpenAICompatible) authorize(req *http.Request) {
	if o.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
}

func (o *OpenAICompatible) endpoint(path string) string {
	copy := *o.base
	copy.Path = strings.TrimRight(copy.Path, "/") + path
	return copy.String()
}

func responseError(operation string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("%s: HTTP %d: %s", operation, resp.StatusCode, strings.TrimSpace(string(body)))
}
