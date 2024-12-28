import uuid
import time
import threading
import random
from datetime import datetime, timedelta
from queue import Queue
import pandas as pd
from product_store import ProductStore

class DataGenerator:
    def __init__(
        self,
        output_file: str = "generated_data.csv",
        rate: float = 0.2,
        num_generators: int = 30,
        min_events: int = 2,  
        max_events: int = 20  
    ):
        self.current_time = datetime(2020, 5, 1)
        self.output_file = output_file
        self.rate = rate
        self.num_generators = num_generators
        self.write_queue = Queue()
        self.min_events = min_events
        self.max_events = max_events
        

        self.output_headers = [
            "event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"
        ]
        pd.DataFrame(columns=self.output_headers).to_csv(
            self.output_file, 
            index=False
        )

    @staticmethod
    def generate_event_type() -> str:
        """Generate a random event type with a specific distribution"""
        return random.choices(
            ["view", "cart", "purchase"],
            weights=[385, 6, 19],
            k=1
        )[0]

    def generate_events(
        self, 
        user_id: int, 
        user_session: str, 
        products: list, 
        rate: float
    ) -> dict:
        """Generate a single event"""
        current_time = datetime.utcnow()
        product = random.choice(products)
        event = {
            "event_time": current_time.strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-4],
            "event_type": self.generate_event_type(),
            **product,
            "user_id": user_id,
            "user_session": user_session,
        }
        return event

    def writer_worker(self):
        """Worker thread to write data to the output file"""
        while True:
            event = self.write_queue.get()
            if event is None:  
                break
            pd.DataFrame([event]).to_csv(
                self.output_file, 
                mode="a", 
                header=False, 
                index=False
            )
            self.write_queue.task_done()

    def data_generator_worker(
        self, 
        user_id: int, 
        user_session: str, 
        rate: float, 
        products: list,
        total_events: int
    ):
        """Worker thread to generate data"""
        events_generated = 0
        
        while events_generated < total_events:
            event = self.generate_events(
                user_id, 
                user_session, 
                products, 
                rate
            )
            self.write_queue.put(event)
            events_generated += 1
            time.sleep(1.0/rate)  

    def run(self, products: list):
        """Run the data generation process"""
        threads = []

        writer_thread = threading.Thread(
            target=self.writer_worker, 
            daemon=True
        )
        writer_thread.start()

        for i in range(self.num_generators):
            user_id = uuid.uuid4().int % 1_000_000_000
            user_session = str(uuid.uuid4())
            thread_rate = self.rate
            
            total_events = random.randint(self.min_events, self.max_events)
            print(f"Generator {i+1} will generate {total_events} events")
            
            thread = threading.Thread(
                target=self.data_generator_worker,
                args=(user_id, user_session, thread_rate, products, total_events)
            )

            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

        self.write_queue.put(None)
        writer_thread.join()

def main():

    store = ProductStore("2019-Nov")
    products = store.get_products()
    

    generator = DataGenerator()
    print("Starting data generation...")
    generator.run(products)
    print("Data generation complete!")

if __name__ == "__main__":
    main()