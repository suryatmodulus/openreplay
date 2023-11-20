from chalicelib.utils import pg_client, helper
from chalicelib.utils.storage import StorageClient
from decouple import config


def get_canvas_presigned_urls(session_id, project_id):
    with pg_client.PostgresClient() as cur:
        cur.execute(cur.mogrify("""\
            SELECT *
            FROM events.canvas_recordings
            WHERE session_id = %(session_id)s
            ORDER BY timestamp;""",
                                {"project_id": project_id, "session_id": session_id})
                    )
        rows = cur.fetchall()

        for i in range(len(rows)):
            rows.append(StorageClient.get_presigned_url_for_sharing(
                bucket=config("CANVAS_BUCKET", default=config("sessions_bucket")),
                expires_in=config("PRESIGNED_URL_EXPIRATION", cast=int, default=900),
                key=rows[i]
            ))
        return rows
