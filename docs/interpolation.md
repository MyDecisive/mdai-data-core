# Interpolation Engine

The interpolation engine replaces placeholders in strings with values from one or more value sources. It is inspired by OpenTelemetry-style attribute interpolation.   
* Placeholders use the form: ${scope:field:-default}   
* Multiple sources may be supplied; one provider per scope is used (first-wins; later duplicates are ignored with a warning).

## Syntax

The interpolation syntax follows a simple pattern:
`${scope:field:-defaultValue}`

- **`scope`**: The source of the data. Currently, the only supported scope is `trigger`.
- **`field`**: The path to the value you want to retrieve from the event. This can be a top-level event attribute or a nested path within the event's JSON payload.
- **`:-defaultValue`**: (Optional) A fallback value to use if the specified `field` cannot be found or its value is considered "empty" (e.g., an empty string or a zero-value timestamp).

---
## Supported Scope: `template`

The `template` scope allows access to predefined key-value pairs.

### Usage

Template values are provided as a `map[string]string` when calling the interpolation function. You can then reference these values using the `template` scope in your interpolation strings.

## Supported Scope: `trigger`

The `trigger` scope refers to the `eventing.MdaiEvent` object that is being processed. Any scope other than `trigger` is unsupported and will cause the placeholder to be returned as-is.

### Top-Level Event Fields

You can access the direct fields of the `MdaiEvent` struct.

**Example:**
`"Event ID is ${trigger:id}"` will be interpolated to `"Event ID is test-id"`.

| Field Name      | `MdaiEvent` Field | Notes                                                              |
|-----------------|-------------------|--------------------------------------------------------------------|
| `id`            | `ID`              | The unique identifier of the event.                                |
| `name`          | `Name`            | The name of the event.                                             |
| `timestamp`     | `Timestamp`       | The event timestamp, formatted as an RFC3339 string (e.g., `2023-01-01T12:00:00Z`). |
| `source`        | `Source`          | The source of the event.                                           |
| `source_id`     | `SourceID`        | The ID from the source system.                                     |
| `correlation_id`| `CorrelationID`   | The correlation ID for tracing.                                    |
| `hub_name`      | `HubName`         | The name of the hub that processed the event.                      |
| `payload`       | `Payload`         | The entire raw JSON payload as a string.                           |

### Payload Fields

To access values within the event's JSON payload, use the `payload.` prefix followed by a dot-separated path.

**Example Event Payload:**
```json
{
  "user": {
    "name": "John",
    "details": {
      "age": 30
    }
  },
  "level": "info",
  "tags": ["a", "b"]
}
```

Example Interpolations:  

| Input String                          | Resulting String     | Description                             |
|---------------------------------------|----------------------|-----------------------------------------|
| `${trigger:payload.level}`            | `info`               | Access a top-level key in the payload.  |
| `${trigger:payload.user.name}`        | `John`               | Access a nested key.                    |
| `${trigger:payload.user.details.age}` | `30`                 | Access a deeply nested key.             |
| `${trigger:payload.tags}`             | `["a","b"]`          | An array is returned as a JSON string.  |
| `${trigger:payload.user}`             | `{"name":"John",...}`| An object is returned as a JSON string. |
| `${trigger:payload.error-rate}`       | `high`               | Hyphenated key supported                |

#### Number formatting
* The payload is decoded with json.Decoder.UseNumber().
* If a number arrives as a json.Number, its lexical form is preserved:
  * ${trigger:payload.n_lex} → 1.00
* Legacy/other numeric values format compactly ('g'):
  * ${trigger:payload.n_int} → 42
  * ${trigger:payload.n_float} → 3.14
### Default  & “absent” semantics
A default (:-value) is used when the field is **present but resolves to “absent”** or when the field path **does not exist** within a supported scope.  

A value is considered **absent** when:
* template value is "" (empty string).
* Event top-level field is empty (e.g., Name == "") or Timestamp is zero.
* JSON path lookup fails within payload (missing key, invalid path, non-object path traversal).
* Payload is invalid JSON (for payload.* retrievals only).

Examples:  

| Input String                               | Condition                               | Resulting String |  
|--------------------------------------------|-----------------------------------------|------------------|  
| `Status: ${trigger:payload.status:-unknown}` | `status` key is missing from payload.   | `Status: unknown`|  
| `Name: ${trigger:name:-guest}`             | `event.Name` is an empty string.        | `Name: guest`    |  
| `Time: ${trigger:timestamp:-now}`          | `event.Timestamp` is a zero time.       | `Time: now`      |  
| `Tag: [${trigger:payload.tag:-}]`          | `tag` key is missing; empty default.    | `Tag: []`        |  

#### Unsupported or missing scope
If the scope has no provider (e.g., no trigger source was supplied, or a typo like env), the placeholder is left unchanged, even if a default is present.
* x=${unknown:key:-d} → x=${unknown:key:-d}  
* x=${trigger:id:-fallback} (no trigger source provided) → x=${trigger:id:-fallback}