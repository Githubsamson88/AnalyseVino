#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uvicorn
from opensi_common.security import token
from fastapi import FastAPI
from starlette.responses import Response
from starlette.requests import Request
from datetime import datetime
from safir.context import Context
from starlette.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from safir.entities.item_entity import ItemEntity
from safir.entities.recipe_entity import RecipeEntity
from safir.entities.step_entity import StepEntity
from safir.entities.sequence_entity import SequenceEntity
from safir.entities.operation_entity import OperationEntity
from safir.entities.action_entity import ActionEntity
from safir.entities.sensor_entity import SensorEntity
from safir.entities.error_entity import ErrorEntity
from safir.services.item_service import ItemService
from safir.services.recipe_service import RecipeService
from safir.services.step_service import StepService
from safir.services.sequence_service import SequenceService
from safir.services.operation_service import OperationService
from safir.services.action_service import ActionService
from safir.services.sensor_service import SensorService
from typing import List
from safir.dao.dao import Dao


ctx = Context()
app = FastAPI(docs_url=ctx.prefix_url + "/docs",
              redoc_url=ctx.prefix_url + "/redoc",
              title="API SAFIR Project",
              version=ctx.version,
              description="Description of the API routes of SAFIR application")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = MongoClient(ctx.mongo_uri)
dao = Dao(mongo_client[ctx.bdd_name])

"""
    *** Version ***
"""
@app.get(ctx.prefix_url + '/version')
def get_version(response: Response):
    response.status_code = 200
    return {"version": "1.0"}


"""
    *** Recipe ***
"""

@app.get(ctx.prefix_url + '/recipe', response_model=List[RecipeEntity], responses={400: {"model": ErrorEntity}})
def get_all_recipe(response: Response, startDate: datetime = None, endDate: datetime = None, finalYield: float = None):
    model_response = RecipeService.find_all_recipe(dao)
    response.status_code = model_response[1]
    return model_response[0]


@app.post(ctx.prefix_url + '/recipe/launchSimulation', response_model=List[StepEntity], responses={400: {"model": ErrorEntity}})
def launch_simulation(response: Response, recipe: RecipeEntity):
    model_response = StepService.find_steps_by_recipe_id(dao, recipe)
    response.status_code = model_response[1]
    return model_response[0]


@app.post(ctx.prefix_url + '/recipe/launchMultipleSimulation', response_model=List[StepEntity], responses={400: {"model": ErrorEntity}})
def launch_multiple_simulation(response: Response, recipe_list: List[RecipeEntity]):
    model_response = StepService.find_steps_by_multiple_recipe(dao, recipe_list)
    response.status_code = model_response[1]
    return model_response[0]


"""
    *** Step ***
"""


@app.get(ctx.prefix_url + '/step/{step_id}', response_model=StepEntity, responses={400: {"model": ErrorEntity}})
def get_step_by_id(response: Response, step_id: str):
    model_response = StepService.find_step_by_id(dao, step_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/step/{step_id}/children', response_model=StepEntity, responses={400: {"model": ErrorEntity}})
def get_step_children_by_id(response: Response, step_id: str):
    model_response = StepService.find_step_children_by_id(dao, step_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/step/name/{step_name}', response_model=List[StepEntity], responses={400: {"model": ErrorEntity}})
def get_steps_by_name(response: Response, step_name: str):
    model_response = StepService.find_steps_by_name(dao, step_name)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/step/name/{step_name}/children', response_model=List[StepEntity], responses={400: {"model": ErrorEntity}})
def get_children_name_by_step_name(response: Response, step_name: str):
    model_response = StepService.find_children_name_by_step_name(dao, step_name)
    response.status_code = model_response[1]
    return model_response[0]


"""
    *** Sequence ***
"""


@app.get(ctx.prefix_url + '/sequence/{sequence_id}', response_model=SequenceEntity, responses={400: {"model": ErrorEntity}})
def get_sequence_by_id(response: Response, sequence_id: str):
    model_response = SequenceService.find_sequence_by_id(dao, sequence_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/sequence/{sequence_id}/children', response_model=SequenceEntity, responses={400: {"model": ErrorEntity}})
def get_sequence_children_by_id(response: Response, sequence_id: str):
    model_response = SequenceService.find_sequence_children_by_id(dao, sequence_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/sequence/name/{sequence_name}', response_model=List[SequenceEntity], responses={400: {"model": ErrorEntity}})
def get_steps_by_name(response: Response, sequence_name: str):
    model_response = SequenceService.find_sequences_by_name(dao, sequence_name)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/sequence/name/{sequence_name}/children', response_model=List[SequenceEntity], responses={400: {"model": ErrorEntity}})
def get_children_name_by_sequence_name(response: Response, sequence_name: str):
    model_response = SequenceService.find_children_name_by_sequence_name(dao, sequence_name)
    response.status_code = model_response[1]
    return model_response[0]


"""
    *** Operation ***
"""


@app.get(ctx.prefix_url + '/operation/{operation_id}', response_model=OperationEntity, responses={400: {"model": ErrorEntity}})
def get_operation_by_id(response: Response, operation_id: str):
    model_response = OperationService.find_operation_by_id(dao, operation_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/operation/{operation_id}/children', response_model=OperationEntity, responses={400: {"model": ErrorEntity}})
def get_operation_children_by_id(response: Response, operation_id: str):
    model_response = OperationService.find_operation_children_by_id(dao, operation_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/operation/name/{operation_name}', response_model=OperationEntity, responses={400: {"model": ErrorEntity}})
def get_operations_by_name(response: Response, operation_name: str):
    model_response = OperationService.find_operations_by_name(dao, operation_name)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/operation/{operation_name}/children', response_model=OperationEntity, responses={400: {"model": ErrorEntity}})
def get_children_name_by_operation_name(response: Response, operation_name: str):
    model_response = OperationService.find_children_name_by_operation_name(dao, operation_name)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/operation/{operation_id}/export', response_model=OperationEntity, responses={400: {"model": ErrorEntity}})
def export_operation_by_id(response: Response, operation_id: str):
    model_response = OperationService.find_operation_by_id(dao, operation_id)
    response.status_code = model_response[1]
    return model_response[0]


"""
    *** Actions ***
"""


@app.get(ctx.prefix_url + '/action/{action_id}', response_model=ActionEntity, responses={400: {"model": ErrorEntity}})
def get_action_by_id(response: Response, action_id: str):
    model_response = ActionService.find_action_by_id(dao, action_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/action/{action_id}/export', response_model=ActionEntity, responses={400: {"model": ErrorEntity}})
def export_action_by_id(response: Response, action_id: str):
    model_response = ActionService.find_action_by_id(dao, action_id)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/action/name/{action_name}', response_model=ActionEntity, responses={400: {"model": ErrorEntity}})
def get_actions_by_name(response: Response, action_name: str):
    model_response = ActionService.find_actions_by_name(dao, action_name)
    response.status_code = model_response[1]
    return model_response[0]


"""
    *** Sensors ***
"""


@app.get(ctx.prefix_url + '/sensor', response_model=SensorEntity, responses={400: {"model": ErrorEntity}})
def get_sensors(response: Response, sensorCode: str = None, recipeId: str = None, sequenceId: str = None, operationId: str = None, sensorType: str = None):
    model_response = SensorService.find_sensors(dao, sensorCode, recipeId, sequenceId, operationId, sensorType)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/sensor/{sensor_id}', response_model=SensorEntity, responses={400: {"model": ErrorEntity}})
def get_sensor_by_id(response: Response, startDate: datetime = None, endDate: datetime = None):
    model_response = SensorService.find_sensor_by_id(dao, startDate, endDate)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/sensor/{sensor_id}/export', response_model=SensorEntity, responses={400: {"model": ErrorEntity}})
def export_sensor_by_id(response: Response, startDate: datetime = None, endDate: datetime = None):
    model_response = SensorService.find_sensor_by_id(dao, startDate, endDate)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/items', response_model=List[ItemEntity], responses={400: {"model": ErrorEntity}})
def find_all_items(request: Request, response: Response):
    #token.check_token_fastapi(request, ctx.opensi_store)
    model_response = ItemService.find_all_item(dao)
    response.status_code = model_response[1]
    return model_response[0]


@app.get(ctx.prefix_url + '/items/{item_id}', response_model=ItemEntity, responses={400: {"model": ErrorEntity}})
def find_item_by_id(request: Request, response: Response, item_id: int):
    #token.check_token_fastapi(request, ctx.opensi_store)
    model_response = ItemService.find_item_by_id(dao, item_id)
    response.status_code = model_response[1]
    return model_response[0]

if __name__ == "__main__":
    uvicorn.run(app, host=ctx.host, port=ctx.port)
