#!/bin/sh
# Install the bundled connector, or build it when run from a source checkout.
set -eu

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/localzie-install.XXXXXX")
install_tmp=
cleanup() {
  status=$?
  trap - 0
  [ -z "$install_tmp" ] || rm -f -- "$install_tmp"
  rm -rf -- "$work_dir"
  if [ "$status" -ne 0 ]; then
    printf '\nLocalzie Connector installation failed. See the error above.\n' >&2
  fi
  exit "$status"
}
trap cleanup 0
trap 'exit 130' 2
trap 'exit 143' 15

printf 'Installing Localzie Connector...\n'
if [ -x "$source_dir/localzie-connector" ]; then
  binary=$source_dir/localzie-connector
elif [ -f "$source_dir/go.mod" ] && [ -d "$source_dir/cmd/localzie-connector" ]; then
  if ! command -v go >/dev/null 2>&1; then
    printf 'Go is needed to build this source checkout. You can download a prebuilt connector from GitHub Releases instead.\n' >&2
    exit 1
  fi
  binary=$work_dir/localzie-connector
  (cd "$source_dir" && go build -trimpath -o "$binary" ./cmd/localzie-connector)
else
  printf 'No connector binary or source checkout was found beside this installer. Extract the complete release archive first.\n' >&2
  exit 1
fi

# Verify this executable runs before replacing an existing installation.
version=$("$binary" version)
if [ -z "${HOME:-}" ]; then
  printf 'Your home directory is unavailable; run the installer from your normal user account.\n' >&2
  exit 1
fi
install_dir=${LOCALZIE_INSTALL_DIR:-}
if [ -z "$install_dir" ]; then
  install_dir=$HOME/.local/bin
  for candidate in "$HOME/.local/bin" "$HOME/go/bin" /opt/homebrew/bin /usr/local/bin; do
    case ":${PATH:-}:" in
      *":$candidate:"*)
        if [ -d "$candidate" ] && [ -w "$candidate" ]; then install_dir=$candidate; break; fi
        ;;
    esac
  done
fi
mkdir -p -- "$install_dir"
install_dir=$(CDPATH= cd -- "$install_dir" && pwd)
install_tmp=$(mktemp "$install_dir/.localzie-connector.XXXXXX")
cp -- "$binary" "$install_tmp"
chmod 755 "$install_tmp"
mv -f -- "$install_tmp" "$install_dir/localzie-connector"
install_tmp=

printf '\nInstalled Localzie Connector (%s).\n' "$version"
printf 'Location: %s/localzie-connector\n\n' "$install_dir"
printf 'Run this command to set up your computer:\n\n'
case ":${PATH:-}:" in
  *":$install_dir:"*) printf '  localzie-connector setup\n' ;;
  *)
    # Quote any spaces or apostrophes so the printed command can be pasted.
    printf "  '"
    printf '%s' "$install_dir/localzie-connector" | sed "s/'/'\\\\''/g"
    printf "' setup\n"
    printf '\nThis folder is not on PATH yet; the full command above works immediately.\n'
    ;;
esac
printf '\nGet Localzie for iPhone: https://apps.apple.com/app/id6813506926\n'
