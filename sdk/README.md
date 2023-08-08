# Sylvia-IoT Go SDK

[![Documentation](https://pkg.go.dev/badge/github.com/woofdogtw/sylvia-iot-go/sdk.svg)](https://pkg.go.dev/github.com/woofdogtw/sylvia-iot-go/sdk)
![CI](https://github.com/woofdogtw/sylvia-iot-go/actions/workflows/build-test.yaml/badge.svg)
[![Coverage](https://raw.githubusercontent.com/woofdogtw/sylvia-iot-go/gh-pages/docs/coverage/sdk/badge.svg)](https://woofdogtw.github.io/sylvia-iot-go/coverage/sdk/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

SDK for developing networks (adapters) and applications on Sylvia-IoT. The SDK contains:

- `api`: utilities for accessing Sylvia-IoT **coremgr** APIs.
- `constants`: Sylvia-IoT constants.
- `middlewares`: middlewares.
    - `auth`: token authentication.
- `mq`: managers for managing network/application connections/queues by using `general-mq`.
