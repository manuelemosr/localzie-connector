// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import "strings"

// ThinkContentFilter removes a model's fallback <think> content while
// retaining ordinary output. It is safe to use on independently sized stream
// chunks, including models that omit the opening delimiter and emit an orphan
// </think> after their private reasoning.
//
// The connector applies this as a final persistence boundary whenever the
// user did not explicitly enable reasoning. Individual runtimes also use it
// when they need to normalize their own response formats.
type ThinkContentFilter struct {
	pending strings.Builder
}

func (f *ThinkContentFilter) Filter(input string) string {
	f.pending.WriteString(input)
	return ""
}

// Flush returns only visible answer text. Buffering until completion is
// deliberate: once untagged text has been forwarded, an orphan closing tag in
// a later chunk cannot be removed from the persisted message. A disabled
// reasoning request therefore trades incremental text for a hard guarantee
// that no chain of thought is stored or displayed.
func (f *ThinkContentFilter) Flush() string {
	text := f.pending.String()
	f.pending.Reset()
	var output strings.Builder
	for text != "" {
		opening := strings.Index(text, "<think>")
		closing := strings.Index(text, "</think>")
		switch {
		case closing >= 0 && (opening < 0 || closing < opening):
			// Qwen may put its private reasoning in content without an opening
			// tag, then terminate it with </think>. Discard every byte leading
			// to that marker; none of it has been persisted yet.
			text = text[closing+len("</think>"):]
			continue
		case opening >= 0:
			output.WriteString(text[:opening])
			text = text[opening+len("<think>"):]
			if end := strings.Index(text, "</think>"); end >= 0 {
				text = text[end+len("</think>"):]
				continue
			}
			// An unfinished opening delimiter has no visible answer after it.
			return output.String()
		default:
			output.WriteString(text)
			return output.String()
		}
	}
	return output.String()
}
