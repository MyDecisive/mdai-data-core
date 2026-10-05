[![Chores](https://github.com/mydecisive/mdai-data-core/actions/workflows/chores.yml/badge.svg)](https://github.com/mydecisive/mdai-data-core/actions/workflows/chores.yml) [![codecov](https://codecov.io/gh/mydecisive/mdai-data-core/graph/badge.svg?token=UPHRBSXOON)](https://codecov.io/gh/mydecisive/mdai-data-core)

# mdai-data-core

## Overview
`mdai-data-core` is a Go library shared by MDAI services. It provides Valkey variable storage, auditing, NATS/JetStream eventing, Kubernetes informers, and OpAMP helpers.

Contributor and agent guidance (commands, invariants, testing rules) lives in [AGENTS.md](AGENTS.md).

It simplifies:
-	**Variable Access**: Conveniently encapsulates and manages variables stored in Valkey.
-	**Audit Management**: Provides robust auditing capabilities for operations performed on Valkey variables and other critical MDAI operations.
-	**Handlers Integration**: Offers streamlined handlers interface for interacting directly with Valkey-stored data.

## Installation
```shell
go get github.com/mydecisive/mdai-data-core
```

## Usage
Basic usage example:

```go
package main

import (
	"context"

	datacore "github.com/mydecisive/mdai-data-core/variables"
)

func main() {
	// initialize your valkeyClient (valkey-go), provide logger
	client := datacore.NewValkeyAdapter(valKeyClient, zapLogger)
	value, found, err := client.GetString(context.TODO(), "your_variable_name", "hub_name")
	// proceed with error handling and the rest of your logic
}
```
## To Generate Mocks
run
```shell
make generate
```