# Changelog

## 0.0.8 - 2023-11-10

### Fixed

- Update dependencies and fix vulnerabilities.

## 0.0.7 - 2023-08-25

### Fixed

- **sdk**: Fix processing token of the auth middleware.

## 0.0.6 - 2023-08-20

### Changed

- **sdk**: **INCOMPATIBLE** API modifications.
    - Modify the `Data` field from `string` to `[]byte` for convenience.

## 0.0.5 - 2023-08-19

### Changed

- **general-mq/sdk**: **INCOMPATIBLE** API modifications.
    - Separates errors, status, messages into three two handlers and three callback functions.

### Fixed

- **sdk**: Fix the dependent version.

## 0.0.4 - 2023-08-12

### Fixed

- **sdk**: Fix the dependent version.

## 0.0.3 - 2023-08-12

### Changed

- Update dependencies.
- **general-mq/sdk**: Provides `persistent` options for AMQP.

## 0.0.2 - 2023-08-11

### Changed

- Update dependencies.

### Fixed

- **general-mq**: Use **persistent** delivery mode when sending data with reliable queues.

## 0.0.1 - 2023-08-08

### Added

- The first release.
