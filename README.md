# Dagster with Data Mesh

The accompanying project to the webinar [Dagster Deep Dives: Enable Data Mesh](https://www.youtube.com/watch?v=laEX2VSq_CQ).

## Getting Started

1. Install the dependencies:

```bash
pip install -e ".[dev]"
```

2. Run Dagster by using the `workspace.yaml` file. This lets you manage running multiple code locations at once. For more information, see the [Dagster documentation](https://docs.dagster.io/concepts/code-locations/workspace-files). To use the `workspace.yaml` file, run the following command:

```bash
dagster dev -w workspace.yaml
```

3. Open the Dagster UI by navigating to [http://localhost:3000](http://localhost:3000).