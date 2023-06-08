from merge_duplicate_companies.celery import app


from .service import merge_companies


@app.task
def merge_run_task(id_company):
    return merge_companies(id_company)

