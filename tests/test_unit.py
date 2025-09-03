from cdk.aws_lambda.lambda_function import create_icechunk_repo, open_icechunk_repo


def test_persistent_virt_container_config(tmp_path):
    """Making sure that the chunk containers are persisted to storage"""
    create_icechunk_repo(str(tmp_path))
    repo = open_icechunk_repo(str(tmp_path))
    assert (
        "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/"
        in repo.config.virtual_chunk_containers.keys()
    )
