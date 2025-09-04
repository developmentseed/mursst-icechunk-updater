#!/usr/bin/env python3
from aws_cdk import App, Tags
from stack import MursstStack
from src.settings import Settings

settings = Settings()

stack_id = f"{settings.stack_name}-{settings.stage}"

app = App()
MursstStack(app, stack_id)

for k, v in dict(
    Project="ODD-MURSST",
    Stack=stack_id,
).items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
