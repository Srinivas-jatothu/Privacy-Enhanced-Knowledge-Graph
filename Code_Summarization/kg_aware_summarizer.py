#!/usr/bin/env python3
"""
kg_aware_summarizer_interactive.py

Asks the user for a file name and prints the corresponding
KG-aware summary if available.
"""

from textwrap import dedent

# Predefined KG-aware summaries
SUMMARIES = {
    "dags/src/anomaly_code_handler.py": dedent(
        """\
        Query: summarize "dags/src/anomaly_code_handler.py"

        Summary (KG-aware):
        This module defines the function
          dags.src.anomaly_code_handler.handle_anomalous_codes,
        a key component in the early stages of the data-cleaning DAG. The function
        identifies and standardises anomalous invoice or transaction codes prior to
        downstream modelling. Within the KG, this node is connected via CALLS and
        DEFINES edges to the central Airflow DAG, and via DEPENDS_ON edges to other
        cleaning and feature-engineering tasks. This situates the function as part of
        a coordinated transformation pipeline rather than an isolated utility.

        Provenance metadata shows that the function was introduced through commit
        f01c5123c9453c3f3972d88a874ee5cdc05c8f16 authored by "Ashkan Ghanavati" and
        merged via pull request #22. The commit summary ("create dags in airflow.py up
        to anomaly_codes_task") indicates that anomaly handling was designed as a
        first-class step in the DAG’s initial architecture. KG enrichment further
        reveals that no subsequent refactor commits modified this file, suggesting
        stability in its function. Such contextual information—developer intent, code
        stability, position in the workflow—cannot be recovered from file content
        alone but becomes explicit through KG-driven summarisation.
        """
    ),

    "dags/src/cancellation_details.py": dedent(
        """\
        Query: summarize "dags/src/cancellation_details.py"

        Summary (KG-aware):
        This module implements
          dags.src.cancellation_details.cancellation_details,
        which enriches transaction records with cancellation-related attributes. The
        KG positions this function within a cluster of behavioural analytics tasks,
        with edges linking it to loaders, outlier handlers, and RFM metric generators.
        This reveals that the cancellation logic forms part of a broader behavioural
        feature-extraction pipeline, rather than being an isolated post-processing
        step.

        The provenance layer shows that the file was introduced by commit
        06d80485b7a621184ec3d4e83a2836f2da6d3387 authored by "Moheth2000" and merged
        via pull request #40. The associated commit summary ("The scripts have been
        added to dags") reflects a bulk integration of multiple data-processing
        components. Unlike the anomaly handler, this file shows multiple MODIFIED_BY
        edges in the KG, indicating later refinements as the analytics pipeline
        evolved. By combining content-level insights with socio-technical metadata,
        the KG enables summarisation that captures not only what the code does, but
        also its historical origin, evolution, and its role in the larger ecosystem.
        """
    ),
}


def main():
    print("=== KG-Aware Code Summarizer ===")
    file_name = input("Enter the file path (e.g., location.filenme.py): ").strip()

    summary = SUMMARIES.get(file_name)

    if summary:
        print("\n" + summary)
    else:
        print(f'\nNo KG-aware summary is defined for "{file_name}".')


if __name__ == "__main__":
    main()
