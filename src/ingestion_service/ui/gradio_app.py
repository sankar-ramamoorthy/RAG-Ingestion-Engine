# src/ingestion_service/ui/gradio_app.py

import os
import json
import requests
import gradio as gr

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001")


def submit_ingest(source_type: str, file_obj):
    try:
        if source_type == "file":
            if file_obj is None:
                return "No file selected."

            metadata = json.dumps(
                {
                    "filename": os.path.basename(file_obj.name),
                }
            )

            with open(file_obj.name, "rb") as f:
                response = requests.post(
                    f"{API_BASE_URL}/v1/ingest/file",
                    files={"file": f},
                    data={"metadata": metadata},
                    timeout=10,
                )
        else:
            response = requests.post(
                f"{API_BASE_URL}/v1/ingest",
                json={
                    "source_type": source_type,
                    "metadata": {},
                },
                timeout=5,
            )

        if response.status_code != 202:
            return f"Error submitting ingestion: {response.text}"

        data = response.json()
        return f"Ingestion accepted.\nID: {data['ingestion_id']}"

    except Exception as exc:
        return f"Error submitting ingestion: {exc}"


def check_status(ingestion_id: str):
    try:
        response = requests.get(
            f"{API_BASE_URL}/v1/ingest/{ingestion_id}",
            timeout=5,
        )

        if response.status_code == 200:
            data = response.json()
            return f"Status: {data['status']}"

        # Any non-200 → show error cleanly
        try:
            error = response.json()
            message = error.get("message", "Unknown error")
        except Exception:
            message = response.text

        return f"Error checking status: {message}"

    except Exception as exc:
        return f"Error checking status: {exc}"


def build_ui():
    with gr.Blocks(title="Agentic RAG Ingestion") as demo:
        gr.Markdown("# Agentic RAG Ingestion (MS2a MVP)")

        with gr.Row():
            source_type = gr.Dropdown(
                choices=["file", "bytes", "uri"],
                value="file",
                label="Source Type",
            )
            file_input = gr.File(label="Upload File")
            submit_btn = gr.Button("Submit Ingestion")

        submission_output = gr.Textbox(label="Submission Result")

        submit_btn.click(
            fn=submit_ingest,
            inputs=[source_type, file_input],
            outputs=submission_output,
        )

        gr.Markdown("## Check Status")
        ingestion_id_input = gr.Textbox(label="Ingestion ID")
        status_btn = gr.Button("Check Status")
        status_output = gr.Textbox(label="Status")

        status_btn.click(
            fn=check_status,
            inputs=ingestion_id_input,
            outputs=status_output,
        )

    return demo


if __name__ == "__main__":
    ui = build_ui()
    ui.launch(server_port=7860, server_name="0.0.0.0")
