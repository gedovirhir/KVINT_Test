import pandas as pd
import numpy as np
import concurrent.futures
import time
import json
import pika

from typing import Optional, Dict, Union, Callable

from pika.channel import Channel
from pika.spec import BasicProperties
from pika.spec import Basic

from pandas import DataFrame
from datetime import datetime

from src.enum import DATA_PATH, REPORTS_INPUT_QUEUE, REPORTS_OUTPUT_QUEUE
from src.models import (CorrelationInput, 
                        CorrelationOutput, 
                        AggregationData, 
                        CorrelationBase,
                        Phone, 
                        Status
                        )

from src.aggregations import AGGREGATIONS, filter_by_phone
from src.core import get_pika_connection
from src.config import config

class ModelEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.int64):
            return int(obj)
        
        elif isinstance(obj, datetime):
            return str(obj)
        
        return super().default(obj)

def load_data():
    return pd.read_json(DATA_PATH)

def run_callables_dict(
    aggs: Dict[str, Union[Callable, dict]], 
    direct_storage: dict, 
    executor: concurrent.futures.Executor, 
    df: pd.DataFrame
):
    put_res = lambda a_name, f, df: direct_storage.update({a_name: f(df)})
    
    for aggr_name, val in aggs.items():
        if isinstance(val, Callable):
            executor.submit(put_res, aggr_name, val, df)
        
        elif isinstance(val, dict):
            new_storage = {}
            direct_storage.update({aggr_name: new_storage})
            run_callables_dict(val, new_storage, executor, df) 

class SingleStorage:
    _storage: Optional[DataFrame] = None
    
    def __new__(cls):
        if not cls._storage:
            data = load_data()
            data['duration'] = (data['end_date'] - data['start_date']) / 1000
            
            cls._storage = data
                   
        return super().__new__(cls)
    
class ReportGenServer(SingleStorage):
    
    def __init__(self):
        self._connection = get_pika_connection(self._on_connection_open)
        self._executor = concurrent.futures.ThreadPoolExecutor()
    
    @property
    def connection(self):
        return self._connection
    
    def _execute_aggegations(self, phone: int) -> Optional[AggregationData]:
        aggr = {
            'phone': phone
        }
        
        with concurrent.futures.ThreadPoolExecutor() as e:    
            df = e.submit(filter_by_phone, self._storage, phone).result()
            if not df.empty:
                run_callables_dict(AGGREGATIONS, aggr, e, df)
        
        res = AggregationData.model_validate(aggr)
        
        return res
    
    def _generate_report(self, body: bytes) -> Optional[CorrelationOutput]:
        task_received = time.time()
        total_duration = lambda: time.time() - task_received
        
        try:
            check_input = CorrelationBase.model_validate_json(body)
        except Exception as ex:
            return None
        
        response_body = {
            'correlation_id': check_input.correlation_id,
            'task_received': datetime.fromtimestamp(task_received)
        }
        
        try:
            corr_input = CorrelationInput.model_validate_json(body)
        except Exception as ex:
            return CorrelationOutput(
                status=Status.REJECT,
                total_duration=total_duration(),
                **response_body
            )
            
        try:
            with concurrent.futures.ThreadPoolExecutor() as e:
                phones_data = e.map(self._execute_aggegations, corr_input.phones)
            
            response_body['status'] = Status.COMPLETE
            response_body['data'] = phones_data
        
        except:
            response_body['status'] = Status.ERROR
        
        res = CorrelationOutput(
            total_duration=total_duration(),
            **response_body
        )
        
        return res
    
    def _process_message(self, channel: Channel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
        report = self._generate_report(body)
        
        if report:
            if report.status == Status.COMPLETE.value:
                channel.basic_ack(delivery_tag=method.delivery_tag)
            
            elif report.status == Status.REJECT.value:
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            
            elif report.status == Status.ERROR.value:
                channel.basic_nack(delivery_tag=method.delivery_tag)

            to_send_body = json.dumps(report.model_dump(), cls=ModelEncoder)
            
        else:
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            to_send_body = "{}"
            
        if properties.reply_to and properties.correlation_id:
            channel.basic_publish(
                exchange='', 
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                ), 
                body=to_send_body
            )
                
        elif to_send_body:
            channel.basic_publish(
                exchange='', 
                routing_key=REPORTS_OUTPUT_QUEUE, 
                body=to_send_body
            )
            
    def _on_message(self, *args, **kwargs):
        self._executor.submit(self._process_message, *args, **kwargs)
    
    def _on_channel_open(self, channel: Channel):
        channel.queue_declare(REPORTS_INPUT_QUEUE, durable=True)
        channel.queue_declare(REPORTS_OUTPUT_QUEUE, durable=True)
        
        channel.basic_qos(prefetch_count=config.PROCESS_COUNT)
        channel.basic_consume(REPORTS_INPUT_QUEUE, on_message_callback=self._on_message)
        
    def _on_connection_open(self, _):
        self._connection.channel(on_open_callback=self._on_channel_open)
    
    def start_pooling(self):
        try:
            self._connection.ioloop.start()
        except Exception as ex:
            self._connection.close()
            self._executor.shutdown()
    
        