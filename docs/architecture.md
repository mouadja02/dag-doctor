# dag-doctor Architecture

## System Components

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Streamlit │────▶│ FastAPI   │────▶│ SQLite   │
│ Dashboard │     │ Backend   │     │ Storage  │
└──────────┘     └────┬─────┘     └──────────┘
                      │
           ┌──────────┼──────────┐
           ▼          ▼          ▼
      ┌────────┐ ┌────────┐ ┌─────────┐
      │Airflow │ │Log     │ │OpenRouter│
      │REST API│ │Parser  │ │LLM      │
      └────────┘ └────────┘ └─────────┘
```

## Data Flow

1. **Fetch**: AirflowClient fetches failed DAG runs and task logs from the Airflow REST API
2. **Parse**: LogParser extracts exception types, tracebacks, and failure signals
3. **Classify**: Classifier matches signals to failure types with confidence scores
4. **Explain**: LLM provider generates root cause analysis and remediation steps
5. **Report**: ReportGenerator produces a Markdown incident report
6. **Store**: Results are persisted in SQLite for later retrieval

## Safety Design

- AirflowClient only uses GET requests — no mutation
- Log parser redacts credentials before analysis
- LLM suggestions are display-only; no auto-execution
- All remediation steps include "what NOT to do" warnings
