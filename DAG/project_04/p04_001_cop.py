from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="hello_world_80",
    description="Test DAG with 80 'hello world' tasks; grouped and with multiple parallel sections",
    start_date=datetime(2024, 1, 1),
    schedule=None,           # Run manually from the UI
    catchup=False,
    tags=["project_04", "example", "test", "hello"],
) as dag:

    # Control nodes for clean fan-in/fan-out (do not count against the 80 hello tasks)
    start = EmptyOperator(task_id="start")
    join1 = EmptyOperator(task_id="join_groups")
    p1_join = EmptyOperator(task_id="join_p1")
    p2_join = EmptyOperator(task_id="join_p2")
    p3_join = EmptyOperator(task_id="join_p3")
    end = EmptyOperator(task_id="end")

    # --- GROUPS ---

    # group_pre: hello_001..hello_010 (parallel within group)
    with TaskGroup("group_pre") as group_pre:
        for i in range(1, 11):
            BashOperator(
                task_id=f"hello_{i:03d}",
                bash_command="echo 'hello world'",
            )

    # group_beta: hello_011..hello_025 (parallel within group)
    with TaskGroup("group_beta") as group_beta:
        for i in range(11, 26):
            BashOperator(
                task_id=f"hello_{i:03d}",
                bash_command="echo 'hello world'",
            )

    # group_alpha: hello_026..hello_040 (parallel within group)
    with TaskGroup("group_alpha") as group_alpha:
        for i in range(26, 41):
            BashOperator(
                task_id=f"hello_{i:03d}",
                bash_command="echo 'hello world'",
            )

    # group_gamma with nested subgroups
    with TaskGroup("group_gamma") as group_gamma:
        with TaskGroup("gamma_subgroup_left") as gamma_left:
            for i in range(41, 46):
                BashOperator(
                    task_id=f"hello_{i:03d}",
                    bash_command="echo 'hello world'",
                )
        with TaskGroup("gamma_subgroup_right") as gamma_right:
            for i in range(46, 51):
                BashOperator(
                    task_id=f"hello_{i:03d}",
                    bash_command="echo 'hello world'",
                )
        # Both nested subgroups run in parallel by default inside group_gamma

    # --- UNGROUPED PARALLEL & CHAINS ---

    # P1 parallel block: hello_051..hello_060
    p1_tasks = [
        BashOperator(task_id=f"hello_{i:03d}", bash_command="echo 'hello world'")
        for i in range(51, 61)
    ]

    # Small linear chain: hello_061 -> hello_062 -> hello_063
    c1_061 = BashOperator(task_id="hello_061", bash_command="echo 'hello world'")
    c1_062 = BashOperator(task_id="hello_062", bash_command="echo 'hello world'")
    c1_063 = BashOperator(task_id="hello_063", bash_command="echo 'hello world'")
    c1_061 >> c1_062 >> c1_063

    # P2 parallel block: hello_064..hello_070
    p2_tasks = [
        BashOperator(task_id=f"hello_{i:03d}", bash_command="echo 'hello world'")
        for i in range(64, 71)
    ]

    # P3 parallel block: hello_071..hello_080
    p3_tasks = [
        BashOperator(task_id=f"hello_{i:03d}", bash_command="echo 'hello world'")
        for i in range(71, 81)
    ]

    # --- DEPENDENCIES / PARALLELISM ---

    # Fan-out 1: two branches after start
    start >> [group_pre, group_beta]

    # Serial flow within each branch
    group_pre >> group_alpha
    group_beta >> group_gamma

    # Join after both branches finish
    [group_alpha, group_gamma] >> join1

    # Fan-out 2: ungrouped parallel P1, then join
    join1 >> p1_tasks >> p1_join

    # Linear chain C1
    p1_join >> c1_061

    # Fan-out 3: P2 parallel, then join
    c1_063 >> p2_tasks >> p2_join

    # Fan-out 4: P3 parallel, then join and finish
    p2_join >> p3_tasks >> p3_join
    p3_join >> end
