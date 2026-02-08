"""
Annotation Server - Updates database with annotation scores
Run: python annotation_server.py
Then open: http://localhost:8001/annotation_tool.html
"""

import http.server
import json
import sqlite3
import os
from urllib.parse import parse_qs
from datetime import datetime

DB_PATH = "../epstein_analysis.db"
PORT = 8001


class AnnotationHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/save_annotations":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode("utf-8"))

            run_id = data.get("run_id")
            score = data.get("score")
            annotations = data.get("annotations", [])

            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # Create annotations table if not exists
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS annotation_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    thread_id TEXT,
                    annotation TEXT,
                    annotated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Update score in ai_classification_runs
            cursor.execute(
                "UPDATE ai_classification_runs SET score = ? WHERE run_id = ?",
                (score, run_id),
            )

            # Save individual annotations
            for ann in annotations:
                cursor.execute(
                    """
                    INSERT INTO annotation_results (run_id, thread_id, annotation)
                    VALUES (?, ?, ?)
                """,
                    (run_id, ann.get("thread_id"), ann.get("annotation")),
                )

            conn.commit()
            conn.close()

            # Send response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {
                        "success": True,
                        "run_id": run_id,
                        "score": score,
                        "annotations_saved": len(annotations),
                    }
                ).encode()
            )
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f"Starting annotation server on http://localhost:{PORT}")
    print(f"Database: {DB_PATH}")
    print(f"Open: http://localhost:{PORT}/annotation_tool.html")
    http.server.HTTPServer(("", PORT), AnnotationHandler).serve_forever()
