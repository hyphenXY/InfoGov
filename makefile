frontend:
	streamlit run app.py --server.enableCORS false --browser.serverAddress 0.0.0.0 --browser.gatherUsageStats false

airflow:
	airflow standalone