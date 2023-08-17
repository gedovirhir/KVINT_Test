from src.service import ReportGenServer

if __name__ == "__main__":
    server = ReportGenServer()
    server.start_pooling()
