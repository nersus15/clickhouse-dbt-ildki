from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from typing import Optional
import os

# --- TASKS ---

@task(name="DBT Connectivity Check")
def check_connectivity(settings: PrefectDbtSettings):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)

    logger.info("Menjalankan 'dbt debug'...")
    try:
        result = runner.invoke(["debug"])
        logger.info(f"Result: {result}")
        if not result.success:
            raise Exception("DBT Debug gagal")
    except Exception as e:
        logger.error("DBT DEBUG CRASHED!")
        logger.error(str(e))
        raise

@task(name="DBT Dependency Validation")
def validate_dependencies(settings: PrefectDbtSettings, selector: Optional[str]):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)
    cmd = ["list"]
    if selector:
        cmd.extend(["--select", selector])
        logger.info(f"Memvalidasi dependensi selector: {selector}")

    result = runner.invoke(cmd)
    if result.success:
        logger.info(f"Model terdeteksi:\n{result.result}")
    else:
        raise Exception("Gagal memvalidasi dependensi.")

@task(name="DBT Core Execution")
def execute_dbt_command(settings: PrefectDbtSettings, command: str, selector: Optional[str] = None,
                        extra_args: Optional[list] = None):
    logger = get_run_logger()
    runner = PrefectDbtRunner(settings=settings)
    # `dbt clean` sengaja tidak dipanggil: menghapus dbt_packages tiap run bikin
    # dbt deps mengunduh ulang -- kontributor OOM.

    full_command = [command]
    if selector:
        full_command.extend(["--select", selector])
    if extra_args:
        full_command.extend(extra_args)

    logger.info(f"Eksekusi: dbt {' '.join(full_command)}")
    result = runner.invoke(full_command)

    if not result.success:
        raise Exception(f"DBT command failed: {result.return_code}")
    return result

# --- FLOW ---

@flow(name="DBT Transformation Runner (ClickHouse)")
def general_dbt_runner(
    dbt_command: str = "build",
    analysis_type: Optional[str] = None,
    include_deps: bool = True
):
    logger = get_run_logger()

    if os.getenv("CLICKHOUSE_PORT"):
        try:
            # Pastikan port bersih dari string kosong/kutip
            port_val = os.getenv("CLICKHOUSE_PORT").replace('"', '').replace("'", "")
            os.environ["CLICKHOUSE_PORT"] = str(int(port_val))
        except ValueError:
            logger.warning("Gagal casting CLICKHOUSE_PORT ke integer, menggunakan nilai default.")

    project_dir = os.path.join(os.getcwd(), "dbt")
    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=project_dir,
    )

    # Logika Selector
    selector = None
    if analysis_type:
        selector = f"staging.{analysis_type}"
        if include_deps:
            selector += "+"
    elif include_deps:
        selector = "staging+"

    # Print env vars untuk debugging (disesuaikan ke variabel ClickHouse)
    logger.info(f"Environment Variables:")
    for key in ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER", "CLICKHOUSE_DATABASE"]:
        logger.info(f"{key}: {os.getenv(key)}")

    # Eksekusi
    check_connectivity(settings)
    validate_dependencies(settings, selector)
    execute_dbt_command(settings, dbt_command, selector)

    logger.info("Pipeline dbt (ClickHouse) selesai.")


@flow(name="DBT Incremental Runner (ClickHouse)")
def incremental_runner(
    select: str = "resource res_link organizations patients encounters conditions observations careplans",
    full_refresh: bool = False,
):
    """Flow runtime terjadwal, khusus model incremental.

    Model DDL (view/MV/dictionary) dijalankan sekali lewat K8s Job, bukan di sini.
    Lebih ramping dari general_dbt_runner: tanpa clean/debug/list, dan pakai
    `dbt run` bukan `dbt build`.

    full_refresh=True (jalankan manual dari Prefect UI, "Custom run") dipakai setelah
    logic/kolom model incremental berubah: rebuild total model `select`, lalu
    rebuild semua view (bisa ikut berubah kolomnya), lalu dbt test. Deployment
    di-set concurrency_limit 1 supaya run terjadwal antre, tidak bentrok.
    """
    logger = get_run_logger()

    if os.getenv("CLICKHOUSE_PORT"):
        try:
            port_val = os.getenv("CLICKHOUSE_PORT").replace('"', '').replace("'", "")
            os.environ["CLICKHOUSE_PORT"] = str(int(port_val))
        except ValueError:
            logger.warning("Gagal casting CLICKHOUSE_PORT ke integer, memakai nilai default.")

    if "+" in select:
        raise ValueError(
            f"Selector '{select}' mengandung '+'. Sebut model incremental secara "
            "eksplisit -- suffix '+' menyeret model MV/view/dictionary ke dalam run."
        )

    project_dir = os.path.join(os.getcwd(), "dbt")
    settings = PrefectDbtSettings(project_dir=project_dir, profiles_dir=project_dir)

    if full_refresh:
        logger.info(f"Full refresh: dbt run --select {select} --full-refresh")
        execute_dbt_command(settings, "run", select, ["--full-refresh"])
        logger.info("Rebuild view: dbt run --select config.materialized:view")
        execute_dbt_command(settings, "run", "config.materialized:view")
        logger.info("Validasi: dbt test")
        execute_dbt_command(settings, "test")
    else:
        logger.info(f"Eksekusi incremental: dbt run --select {select}")
        execute_dbt_command(settings, "run", select)

    logger.info("Pipeline dbt incremental (ClickHouse) selesai.")
