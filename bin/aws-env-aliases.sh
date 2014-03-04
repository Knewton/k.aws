# Example aliases
alias saws='aws-env -e staging > ~/.k.aws/$$.currenv ; source ~/.k.aws/$$.currenv ; rm ~/.k.aws/$$.currenv ; echo "${AWS_ACCOUNT}"'
alias paws='aws-env -e production > ~/.k.aws/$$.currenv ; source ~/.k.aws/$$.currenv ; rm ~/.k.aws/$$.currenv ; echo "${AWS_ACCOUNT}"'
