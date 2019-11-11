#!/bin/bash -xue

ZCL_VERSION=0.4.0
curl -sL https://github.com/zeebe-io/zeebe-changelog/releases/download/${ZCL_VERSION}/zeebe-changelog_${ZCL_VERSION}_Linux_x86_64.tar.gz | tar xzvf - -C /usr/bin zcl

chmod +x /usr/bin/zcl

label="Release: ${RELEASE_VERSION}"

zcl add-labels --label "${label}" --from "${PREVIOUS_VERSION}" --target "${RELEASE_VERSION}"
zcl generate --label "${label}" > /tmp/CHANGELOG

cat /tmp/CHANGELOG
