from merge_duplicate_companies.celery import app


from .service import merge_companies
from duplicate_company_resolver.main import find_and_merge_duplicates


@app.task
def merge_run_task(id_company):
    return merge_companies(id_company)


# Объединение с использованием функционала Битрикс
@app.task
def merge_run_task_2(id_company):
    return find_and_merge_duplicates(id_company)

