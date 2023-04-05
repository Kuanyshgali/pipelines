from pipelines import tasks, Pipeline


NAME = 'test_project'
VERSION = '2023'


TASKS = [
    # tasks.RunSQL('drop table `domain`'),
    # tasks.RunSQL('drop table `norm`'),

    tasks.LoadFile(input_file='example_pipeline/original/original.csv', table='domain'),

    tasks.CTAS(
        table='norm',
        columnNum='column2',
        sql_query="""
            select *, domain_of_url(column2)
            from `domain`;
        """
    ),
    tasks.CopyToFile(
        table='norm',
        output_file='norm',
    ),

# clean up:
    tasks.RunSQL('drop table `domain`'),
    tasks.RunSQL('drop table `norm`'),

]


pipeline = Pipeline(
    name=NAME,
    version=VERSION,
    tasks=TASKS
)


if __name__ == "__main__":
    # 1: Run as script
    pipeline.run()

    # 2: Run as CLI
    # > pipelines run
