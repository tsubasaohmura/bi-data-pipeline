from airflow.hooks.base_hook import BaseHook
import re
import subprocess


class MarklogicHook(BaseHook):
    """Very basic hook to MarkLogic. 
    Reads info from a specified connection, and allows to execute MLCP to that connection.
    
    PROPERTIES:
        marklogic_conn_id (str): name of MarkLogic connection in Connections

    FUNCTIONS:
        _scrape_command_result(process_output): returns output of mlcp command
        get_conn(): get Marklogic Connection information
        mlcp_json(): get connection and send query to HANA database
    """
    
    default_conn_name = 'marklogic_default'

    def __init__(self, marklogic_conn_id=default_conn_name):
        super().__init__(source=None)
        self.marklogic_conn_id = marklogic_conn_id


    def _scrape_command_result(self, process_output):
        assert isinstance(process_output, subprocess.CompletedProcess)
        # group(0) means full regex match. group(1) means the value in ( ) only.
        result = {
            'total_paths': re.search("Total input paths to process : (\d+)", process_output.stderr).group(1),
            'input_records': re.search("INPUT_RECORDS: (\d+)", process_output.stderr).group(1),
            'output_records': re.search("OUTPUT_RECORDS: (\d+)", process_output.stderr).group(1),
            'output_records_committed': re.search("OUTPUT_RECORDS_COMMITTED: (\d+)", process_output.stderr).group(1),
            'output_records_failed': re.search("OUTPUT_RECORDS_FAILED: (\d+)", process_output.stderr).group(1),
        }

        return result


    def get_conn(self):
        """Return connection information"""
        connection = self.get_connection(self.marklogic_conn_id)
        return connection


    def mlcp_import_json(self, *, input_path, output_path, collections,
    output_permissions="read-role,read", thread_count=48):
        """Import files to Marklogic using mlcp.sh"""
        
        conn = self.get_conn()

        if output_path.endswith('/'):
            output_path = output_path[:-1]

        import_command = [
            "/opt/MarkLogic/mlcp/bin/mlcp.sh",
            "import",
            "-ssl",
            "-host", conn.host,
            "-port", str(conn.port),
            "-username", conn.login,
            "-password", conn.get_password(),
            "-database", conn.schema,
            "-input_file_path", input_path,
            "-input_file_type", "delimited_json",
            "-generate_uri", "true",
            "-output_uri_replace", f"{input_path},'{output_path}'",
            "-output_collections", collections,
            "-output_permissions", output_permissions,
            "-thread_count", str(thread_count),
        ]

        # check argument checks if return code is 0, if not - raises error
        command_result = subprocess.run(import_command, universal_newlines=True, check=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = self._scrape_command_result(command_result)
        report = f"""
        = MLCP =
        Parameters:
            Input path: {input_path}
            Output path: {output_path}
            Collections: {collections}
        Result:
            Total input paths to processed: {result['total_paths']}
            INPUT_RECORDS: {result['input_records']}
            OUTPUT_RECORDS: {result['output_records']}
            OUTPUT_RECORDS_COMMITTED: {result['output_records_committed']}
            OUTPUT_RECORDS_FAILED: {result['output_records_failed']}
        """
        self.log.info(report)

        if int(result['output_records_failed']) != 0:
            raise RuntimeError("Upload to MarkLogic unsuccessful. See details in log.")

        return result
