#!/usr/bin/env bash
set -euo pipefail

# Publish a clean copy of the current project into a target repository under project/.
#
# Usage:
#   scripts/publish_clean_copy_to_supernotes.sh \
#     --repo https://github.com/David-Wu1119/SuperNotes.git \
#     --branch codex/project-sync-20260301 \
#     --message "Add clean trading project snapshot"
#
# Notes:
# - Requires git + rsync.
# - Uses scripts/clean_copy_excludes.txt for filtering.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
EXCLUDE_FILE="${SCRIPT_DIR}/clean_copy_excludes.txt"

TARGET_REPO_URL="https://github.com/David-Wu1119/SuperNotes.git"
TARGET_SUBDIR="project"
BRANCH_NAME="codex/project-sync-$(date +%Y%m%d-%H%M%S)"
COMMIT_MESSAGE="Add clean project copy from NLP_Final_Project_D"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      TARGET_REPO_URL="${2:?missing value for --repo}"
      shift 2
      ;;
    --branch)
      BRANCH_NAME="${2:?missing value for --branch}"
      shift 2
      ;;
    --message)
      COMMIT_MESSAGE="${2:?missing value for --message}"
      shift 2
      ;;
    --subdir)
      TARGET_SUBDIR="${2:?missing value for --subdir}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${EXCLUDE_FILE}" ]]; then
  echo "Exclude file not found: ${EXCLUDE_FILE}" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git not found" >&2
  exit 1
fi
if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "Cloning target repo: ${TARGET_REPO_URL}"
git clone --depth 1 "${TARGET_REPO_URL}" "${TMP_DIR}/target"

pushd "${TMP_DIR}/target" >/dev/null

if git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}"; then
  git checkout "${BRANCH_NAME}"
else
  git checkout -b "${BRANCH_NAME}"
fi

mkdir -p "${TARGET_SUBDIR}"

echo "Syncing clean copy into ${TARGET_SUBDIR}/ ..."
set +e
rsync -a --delete --delete-excluded \
  --exclude-from="${EXCLUDE_FILE}" \
  "${SOURCE_ROOT}/" "${TARGET_SUBDIR}/"
RSYNC_EXIT=$?
set -e

# rsync 23/24 may occur due partial transfer/unreadable files; for this export
# workflow we tolerate them when the target tree is still produced.
if [[ ${RSYNC_EXIT} -ne 0 && ${RSYNC_EXIT} -ne 23 && ${RSYNC_EXIT} -ne 24 ]]; then
  echo "rsync failed with code ${RSYNC_EXIT}" >&2
  exit "${RSYNC_EXIT}"
fi

if [[ ${RSYNC_EXIT} -eq 23 || ${RSYNC_EXIT} -eq 24 ]]; then
  echo "Warning: rsync reported partial transfer (code ${RSYNC_EXIT}); continuing."
fi

git add "${TARGET_SUBDIR}"

if git diff --cached --quiet; then
  echo "No changes to commit in ${TARGET_SUBDIR}/"
  popd >/dev/null
  exit 0
fi

git commit -m "${COMMIT_MESSAGE}"
git push -u origin "${BRANCH_NAME}"

CURRENT_HEAD="$(git rev-parse --short HEAD)"
popd >/dev/null

echo "Done."
echo "Repo: ${TARGET_REPO_URL}"
echo "Branch: ${BRANCH_NAME}"
echo "Commit: ${CURRENT_HEAD}"
