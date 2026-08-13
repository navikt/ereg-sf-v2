import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

/*
    A stand-alone browser for /tmp/files
    Example reference http4k:
            "/internal/files" bind Method.GET to filesHandler(File("/tmp/files")),
            "/internal/files/{path:.*}" bind Method.GET to filesHandler(File("/tmp/files")),

    Also apply on boot:
            val dir = File("/tmp/files")
            dir.mkdirs() // ensures /tmp/files exists
 */
fun filesHandler(baseDir: File): HttpHandler =
    { request ->
        val path =
            request.uri.path
                .removePrefix("/internal/files")
                .trim('/')

        val target = if (path.isEmpty()) baseDir else File(baseDir, path)

        // Prevent path traversal attacks
        val canonicalBase = baseDir.canonicalFile
        val canonicalTarget = target.canonicalFile

        if (!canonicalTarget.path.startsWith(canonicalBase.path)) {
            Response(Status.FORBIDDEN).body("Forbidden")
        } else if (!target.exists()) {
            Response(Status.NOT_FOUND).body("Not found")
        } else if (target.isDirectory) {
            val files =
                target
                    .listFiles()
                    ?.sortedWith(compareBy<File> { !it.isDirectory }.thenBy { it.name.lowercase() })
                    ?: emptyList()

            val parentPath =
                request.uri.path
                    .trimEnd('/')
                    .substringBeforeLast('/', "/")
                    .ifEmpty { "/" }

            val html =
                buildString {
                    append(
                        """
                        <html>
                        <head>
                            <meta charset="UTF-8">
                            <title>File Browser</title>

                            <style>
                                body {
                                    background-color: #dddddd;
                                    font-family: Arial, sans-serif;
                                    margin: 0;
                                    padding: 20px;
                                }

                                #project-title {
                                    font-size: 28px;
                                    color: #444;
                                    text-align: center;
                                    margin-bottom: 30px;
                                }

                                .dataset-section {
                                    margin: 20px auto;
                                    padding: 15px;
                                    border: 2px solid #ccc;
                                    border-radius: 10px;
                                    background-color: #f5f5f5;
                                    max-width: 1000px;
                                }

                                .dataset-header {
                                    color: #0e2a3d;
                                    font-size: 24px;
                                    font-weight: bold;
                                    margin-bottom: 15px;
                                    padding-left: 20px;
                                    word-break: break-all;
                                }

                                .table {
                                    margin-left: 20px;
                                    margin-right: 20px;
                                    margin-bottom: 10px;
                                }

                                .table-header {
                                    cursor: pointer;
                                    font-size: 18px;
                                    background-color: #3498DB;
                                    color: white;
                                    padding: 12px;
                                    border-radius: 5px;

                                    display: flex;
                                    justify-content: space-between;
                                    align-items: center;

                                    text-decoration: none;
                                    transition: background-color 0.15s ease-in-out;
                                }

                                .table-header:hover {
                                    background-color: #2980B9;
                                }

                                .name-and-label-wrapper {
                                    display: flex;
                                    align-items: center;
                                    gap: 10px;
                                }

                                .directory-label {
                                    font-family: Arial, sans-serif;
                                    background-color: #c6e8cd;
                                    color: #1f7a4d;
                                    padding: 4px 6px;
                                    border-radius: 5px;
                                    font-size: 12px;
                                }

                                .file-meta {
                                    color: #d9eefb;
                                    font-size: 13px;
                                    font-family: monospace;
                                }

                                .file-view {
                                    background-color: #f9f9f9;
                                    border: 1px solid #ccc;
                                    border-radius: 10px;
                                    padding: 20px;
                                    margin-top: 20px;
                                    white-space: pre-wrap;
                                    word-wrap: break-word;
                                    overflow-x: auto;
                                    font-family: monospace;
                                    color: #333;
                                }
                            </style>
                        </head>

                        <body>
                            <div id="project-title">File Browser</div>

                            <div class="dataset-section">
                                <div class="dataset-header">
                                    Index of ${request.uri.path}
                                </div>
                        """.trimIndent(),
                    )

                    if (path.isNotEmpty()) {
                        append(
                            """
                            <div class="table">
                                <a href="$parentPath" class="table-header">
                                    <div class="name-and-label-wrapper">
                                        ../
                                    </div>
                                </a>
                            </div>
                            """.trimIndent(),
                        )
                    }

                    files.forEach { file ->
                        val isDirectory = file.isDirectory
                        val name = file.name + if (isDirectory) "/" else ""
                        val link = "${request.uri.path.trimEnd('/')}/$name"

                        val sizeText =
                            if (isDirectory) {
                                ""
                            } else {
                                "${file.length()} bytes"
                            }

                        append(
                            """
                            <div class="table">
                                <a href="$link" class="table-header">
                                    <div class="name-and-label-wrapper">
                                        <span>$name</span>
                                        ${
                                if (isDirectory) {
                                    """<span class="directory-label">DIR</span>"""
                                } else {
                                    ""
                                }
                            }
                                    </div>

                                    <div class="file-meta">
                                        $sizeText
                                    </div>
                                </a>
                            </div>
                            """.trimIndent(),
                        )
                    }

                    append(
                        """
                            </div>
                        </body>
                        </html>
                        """.trimIndent(),
                    )
                }

            Response(Status.OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(html)
        } else {
            val escapedContent =
                target
                    .readText()
                    .replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")

            val parentPath =
                request.uri.path
                    .trimEnd('/')
                    .substringBeforeLast('/', "/")
                    .ifEmpty { "/" }

            val html =
                """
                <html>
                <head>
                    <meta charset="UTF-8">
                    <title>${target.name}</title>

                    <style>
                        body {
                            background-color: #dddddd;
                            font-family: Arial, sans-serif;
                            margin: 0;
                            padding: 20px;
                        }

                        .dataset-section {
                            margin: 20px auto;
                            padding: 15px;
                            border: 2px solid #ccc;
                            border-radius: 10px;
                            background-color: #f5f5f5;
                            max-width: 1200px;
                        }

                        .dataset-header {
                            color: #0e2a3d;
                            font-size: 24px;
                            font-weight: bold;
                            margin-bottom: 15px;
                        }

                        .back-link {
                            display: inline-block;
                            margin-bottom: 20px;
                            text-decoration: none;
                            background-color: #3498DB;
                            color: white;
                            padding: 10px 14px;
                            border-radius: 5px;
                        }

                        .back-link:hover {
                            background-color: #2980B9;
                        }

                        .file-view {
                            background-color: #f9f9f9;
                            border: 1px solid #ccc;
                            border-radius: 10px;
                            padding: 20px;
                            white-space: pre-wrap;
                            word-wrap: break-word;
                            overflow-x: auto;
                            font-family: monospace;
                            color: #333;
                        }
                    </style>
                </head>

                <body>
                    <div class="dataset-section">

                        <a href="$parentPath" class="back-link">
                            ← Back
                        </a>

                        <div class="dataset-header">
                            ${target.name}
                        </div>

                        <div class="file-view">$escapedContent</div>
                    </div>
                </body>
                </html>
                """.trimIndent()

            Response(Status.OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(html)
        }
    }
