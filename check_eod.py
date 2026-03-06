from core.database import DatabaseManager

db = DatabaseManager()
db.connect()

today = "2026-03-05"

eod_deleted = db.delete_old_eod_volumes(keep_date=today)
alert_deleted = db.delete_old_alerts(keep_date=today)

print("Deleted old EOD records:", eod_deleted, "(kept", today + ")")
print("Deleted old alert records:", alert_deleted, "(kept", today + ")")

dates_eod = db._eod_volumes.distinct("date")
dates_alerts = db._alerts.distinct("date")
print("EOD dates remaining:", dates_eod)
print("Alert dates remaining:", dates_alerts)
print("EOD symbols for today:", db._eod_volumes.count_documents({"date": today}))
print("Alerts for today:", db._alerts.count_documents({"date": today}))
