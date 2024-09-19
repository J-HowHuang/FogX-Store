from skulk.predatorfox.client import PredatorfoxClient, SkulkQuery


    
if __name__ == "__main__":
    client = PredatorfoxClient("[::1]:50051")
    client.query(SkulkQuery(dataset="empty_table", columns=[]))