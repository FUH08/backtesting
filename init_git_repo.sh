#!/usr/bin/env bash
# 在已安装 git 的机器上运行：bash init_git_repo.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if ! command -v git >/dev/null 2>&1; then
  echo "未找到 git，请先安装（例如: sudo apt install git）"
  exit 1
fi

if [[ -d .git ]]; then
  echo "当前目录已是 git 仓库。如需重新初始化请先删除 .git 目录（会丢失本地提交历史）。"
  git status -sb
  exit 0
fi

git init
git branch -M main
git add -A
git status
git commit -m "Initial commit: backtesting system (DolphinDB + lightweight-charts plotting)"

echo ""
echo "本地仓库已创建并完成首次提交（分支 main）。"
echo "在 GitHub/GitLab/Gitee 新建空仓库后执行："
echo "  git remote add origin <你的仓库 HTTPS 或 SSH URL>"
echo "  git push -u origin main"
