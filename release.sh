#!/bin/sh

if [ -z "$1" ]; then
    echo "Usage: release.sh TAG"
    return
fi

swpt_debtors="epandurski/swpt_debtors:$1"
swpt_debtors_swagger_ui="epandurski/swpt_debtors_swagger_ui:$1"
docker build -t "$swpt_debtors" --target app-image .
docker build -t "$swpt_debtors_swagger_ui" --target swagger-ui-image .
git tag "v$1"
git push origin "v$1"
docker login
docker push "$swpt_debtors"
docker push "$swpt_debtors_swagger_ui"
