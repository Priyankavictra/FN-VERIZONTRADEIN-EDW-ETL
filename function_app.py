
import logging
import azure.functions as func
from verizon_etl import VerizonTradeInETL
from datetime import datetime, timedelta
import json

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def fnverizonetltimer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    # Default params for timer
    params = {
        "ProviderID": "18",
        "CompanyID": "13325",
        "LocationType": "Company",
        "LocationTypeIDs": "-1",
        "StartDate": "2025-07-25",
        "StopDate": "2025-07-26"
    }
    etl = VerizonTradeInETL()
    result = etl.run_etl(params)
    logging.info(f'Timer ETL result: {result}')

@app.function_name(name="fnverizonetlmanual")
@app.route(route="verizonetlmanual", methods=["GET", "POST"])
def fnverizonetlmanual(req: func.HttpRequest) -> func.HttpResponse:
    try:
        params = {
            "ProviderID": req.params.get("ProviderID"),
            "CompanyID": req.params.get("CompanyID"),
            "LocationType": req.params.get("LocationType"),
            "LocationTypeIDs": req.params.get("LocationTypeIDs"),
            "StartDate": req.params.get("StartDate"),
            "StopDate": req.params.get("StopDate")
        }
        try:
            req_body = req.get_json()
            for k in params:
                if not params[k] and k in req_body:
                    params[k] = req_body[k]
        except Exception:
            pass
        # Always use Jira date range
        params["StartDate"] = "2025-07-25"
        params["StopDate"] = "2025-07-26"
        if not params["ProviderID"]:
            params["ProviderID"] = "18"
        if not params["CompanyID"]:
            params["CompanyID"] = "13325"
        if not params["LocationType"]:
            params["LocationType"] = "Company"
        if not params["LocationTypeIDs"]:
            params["LocationTypeIDs"] = "-1"
        etl = VerizonTradeInETL()
        result = etl.run_etl(params)
        return func.HttpResponse(json.dumps(result), status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)