#!/usr/bin/env bash
# Points this clone's git hooks at tool/hooks (pre-push: analyze + Dart tests).
# Usage: bash tool/install_hooks.sh   (once per clone)
# Undo:  git config --unset core.hooksPath
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
chmod +x tool/hooks/*
git config core.hooksPath tool/hooks
echo "Git hooks installed from tool/hooks (core.hooksPath set for this clone)."
