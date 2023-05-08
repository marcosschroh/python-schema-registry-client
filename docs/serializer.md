# Serializers

To serialize and deserialize messages you can use `AvroMessageSerializer` and `JsonMessageSerializer`. They interact with the `SchemaRegistryClient` to get `avro Schemas`  and `json schemas` in order to process messages.

*If you want to run the following examples run `docker-compose up` and the `schema registry server` will run on `http://127.0.0.1:8081`*

!!! warning
    The `AvroMessageSerializer` uses the same `protocol` as confluent, meaning that the event will contain the schema id in the payload.
    If you produce an event with the `AvroMessageSerializer` you have to consume it with the `AvroMessageSerializer` as well, otherwise you have to
    implement the parser on the consumer side.

::: schema_registry.serializers.AvroMessageSerializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: true

::: schema_registry.serializers.JsonMessageSerializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_base_classes: true

## Async implementations

`JsonMessageSerializer`, `AvroMessageSerializer` and `SchemaRegistryClient` have their asynchronous
counterparts `AsyncJsonMessageSerializer`, `AsyncAvroMessageSerializer` and `AsyncSchemaRegistryClient` and all
examples above should work if you replace them with their async variations

::: schema_registry.serializers.AsyncAvroMessageSerializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false

::: schema_registry.serializers.AsyncJsonMessageSerializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
