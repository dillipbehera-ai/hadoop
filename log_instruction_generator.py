# coding: utf-8
"""Utility to generate instructions for failed Airflow tasks based on logs."""

from __future__ import annotations

import os
import re
from typing import List

import openai


PATTERNS = {
    r"OutOfMemoryError": "The Spark job ran out of memory. Increase executor memory or reduce data size.",
    r"No space left on device": "The cluster does not have enough disk space. Free up space or use a larger instance.",
    r"Permission denied": "A file or directory could not be accessed due to permission issues. Check IAM roles and file permissions.",
    r"Connection refused": "Spark could not connect to a required service. Verify network settings and service endpoints.",
    r"SparkException": "Spark reported a generic error. Inspect earlier log entries for a more specific cause.",
    r"Task failed with exit status (\d+)": "A Spark task failed. Review the executor logs for stack traces and memory errors.",
}


def _match_patterns(log: str) -> List[str]:
    """Return troubleshooting messages that match patterns in the log."""
    messages = []
    for pattern, msg in PATTERNS.items():
        if re.search(pattern, log, re.IGNORECASE):
            if pattern == r"Task failed with exit status (\d+)":
                match = re.search(pattern, log)
                if match:
                    messages.append(msg.replace("(\\d+)", match.group(1)))
            else:
                messages.append(msg)
    return messages


def _chatgpt_analysis(log: str) -> str:
    """Return ChatGPT analysis of the failure if API key is available."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "OpenAI API key not configured; skipping ChatGPT analysis."

    openai.api_key = api_key
    system_prompt = (
        "You are an expert at analysing Apache Airflow and Spark logs. "
        "Summarise why the job failed, provide root cause analysis, and "
        "suggest code changes to resolve it in no more than 50 lines."
    )
    user_prompt = f"Log excerpt:\n{log}"

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
    except Exception as exc:  # pragma: no cover - runtime environment may vary
        return f"ChatGPT request failed: {exc}"

    content = response["choices"][0]["message"]["content"].strip()
    return "\n".join(content.splitlines()[:50])


def generate_failure_report(log: str) -> str:
    """Generate up to 50 lines of instructions based on an Airflow log.

    Parameters
    ----------
    log : str
        The full log output from a failed Airflow task, including Spark logs.

    Returns
    -------
    str
        A multiline string (\n separated) with up to 50 lines describing the
        reason for failure, root cause analysis, and suggestions to resolve it.
        If ``OPENAI_API_KEY`` is set, results also include ChatGPT guidance on
        potential code changes.
    """
    instructions: List[str] = []

    if not log:
        return "No log information provided."  # early exit

    # Detect known patterns and add corresponding messages
    instructions.extend(_match_patterns(log))

    # Append ChatGPT analysis if possible
    instructions.extend(_chatgpt_analysis(log).splitlines())

    # Provide a generic instruction if nothing matched
    if not instructions:
        instructions.append(
            "Could not determine a specific cause. Check the stack trace and "
            "verify Spark configuration, resource allocation, and input data."
        )

    instructions.append(
        "For more detailed analysis, review the full executor logs and Airflow "
        "task output around the failure time."
    )

    # Trim to a maximum of 50 lines
    instructions = instructions[:50]

    return "\n".join(instructions)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            log_content = f.read()
    else:
        log_content = sys.stdin.read()
    print(generate_failure_report(log_content))
