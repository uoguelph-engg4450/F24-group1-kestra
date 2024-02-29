## Introduction

Welcome to Kestra!

Kestra is a universal orchestration platform that allows you to automate complex workflows using a simple declarative interface.

This guide lists the core properties that you can use to define your flows.

# Flow properties

This section describes the properties that you can use in your flows.

> Flows define tasks, the execution order of tasks, as well as flow inputs, variables, labels, and triggers.

Flows are defined in a declarative YAML syntax to keep the orchestration code portable and language-agnostic.

A flow **must** have an identifier (id), a namespace, and a list of tasks. Those are the only required properties. All other properties are optional.

The **optional properties** include a description, labels, inputs, variables, triggers, and task defaults.

The table below describes each of these properties in detail.

| Field                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                               |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `id`                    | The flow identifier which represents the name of the flow. This ID must be unique within a namespace and is immutable (you cannot rename the flow ID later; you could only recreate it with a new name).                                                                                                                                                                                                                                  |
| `namespace`             | Each flow lives in one namespace. Namespaces are used to group flows and provide structure. Allocation of a flow to a namespace is immutable. Once a flow is created, you cannot change its namespace. If you need to change the namespace of a flow, create a new flow with the desired namespace and delete the old flow.                                                                                                               |
| `revision`              | The flow version, handled internally by Kestra, and incremented upon each modification. You should **not** manually set it.                                                                                                                                                                                                                                                                                                               |
| `description`           | The description of the flow.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `labels`                | Key-value pairs that you can use to organize your flows based on your project, maintainers, or any other criteria. You can use labels to filter Executions in the UI.                                                                                                                                                                                                                                                                     |
| `inputs`                | The list of inputs that allow you to make your flows more dynamic and reusable. Instead of hardcoding values in your flow, you can use inputs to trigger multiple Executions of your flow with different values determined at runtime. Use the syntax `{{ inputs.your_input_name }}` to access specific input values in your tasks.                                                                                                       |
| `variables`             | The list of variables (such as an API endpoint, table name, download URL, etc.) that you can access within tasks using the syntax `{{ vars.your_variable_name }}`.                                                                                                                                                                                                                                                                        |
| `tasks`                 | The list of tasks to be executed within the flow. Tasks are atomic actions in your flows. By default, they will run sequentially one after the other. However, you can use additional [Flowable](https://kestra.io/docs/tutorial/flowable) tasks to run some tasks in parallel.                                                                                                                                                           |
| `errors`                | The list of error tasks, all listed tasks will be run sequentially only if there is an error on the current execution.                                                                                                                                                                                                                                                                                                                    |
| `listeners`             | The list of listeners (deprecated).                                                                                                                                                                                                                                                                                                                                                                                                       |
| `triggers`              | The list of triggers which automatically start a flow execution based on events, such as a scheduled date, a new file arrival, a new message in a queue, or the completion event of another flow's execution.                                                                                                                                                                                                                             |
| `taskDefaults`          | The list of default task values, allowing you to avoid repeating the same properties on each task.                                                                                                                                                                                                                                                                                                                                        |
| `taskDefaults.[].type`  | The task type is a full qualified Java class name, i.e. the task name such as `io.kestra.core.tasks.log.Log`.                                                                                                                                                                                                                                                                                                                             |
| `taskDefaults.[].forced`| If set to `forced: true`, the `taskDefault` will take precedence over properties defined in the task (the default behavior is `forced: false`).                                                                                                                                                                                                                                                                                           |
| `taskDefaults.[].values.xxx`| The task property that you want to be set as default.                                                                                                                                                                                                                                                                                                                                                                                     |
| `disabled`              | Set it to `true` to temporarily disable any new executions of the flow. This is useful when you want to stop a flow from running (even manually) without deleting it. Once you set this property to true, nobody will be able to trigger any execution of that flow, whether from the UI or via an API call, until the flow is reenabled by setting this property back to `false` (default behavior) or by simply deleting this property. |


---

## Flow example

Here is an example flow. It uses a `Log` task available in Kestra core for testing purposes and demonstrates how to use `labels`, `inputs` and `variables`, `triggers` and various `descriptions`.

```yaml
id: getting_started
namespace: dev

description: Let's `write` some **markdown** - [first flow](https://t.ly/Vemr0) 🚀

labels:
  owner: rick.astley
  project: never-gonna-give-you-up
  environment: dev
  country: US

inputs:
  - name: user
    type: STRING
    required: false
    defaults: Rick Astley
    description: This is an optional input. If not set at runtime, it will use the default value "Rick Astley".

variables:
  first: 1
  second: "{{vars.first}} > 2"

tasks:
  - id: hello
    type: io.kestra.core.tasks.log.Log
    description: this is a *task* documentation
    message: |
      The variables we used are {{vars.first}} and {{vars.second}}.
      The input is {{inputs.user}} and the task was started at {{taskrun.startDate}} from flow {{flow.id}}.

taskDefaults:
  - type: io.kestra.core.tasks.log.Log
    values:
      level: TRACE

triggers:
  - id: monthly
    type: io.kestra.core.models.triggers.types.Schedule
    cron: "0 9 1 * *" # every first day of the month at 9am
```

You can add documentation to flows, tasks, inputs or triggers using the `description` property where you can use the [Markdown](https://en.wikipedia.org/wiki/Markdown) syntax. All markdown descriptions will be rendered in the UI.


## Links to learn more

* Follow the step-by-step [tutorial](https://kestra.io/docs/tutorial)
* Check the [documentation](https://kestra.io/docs)
* Watch a 10-minute video explanation of key concepts on the [Kestra's YouTube channel](https://youtu.be/yuV_rgnpXU8?si=tdMnZlovgHgnwx0K)
* Submit a feature request or a bug report on [GitHub](https://github.com/kestra-io/kestra/issues/new/choose)
* Need help? [Join the community](https://kestra.io/slack)
* Do you like the project? Give us a ⭐️ [GitHub star](https://github.com/kestra-io/kestra)
