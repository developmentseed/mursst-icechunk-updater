#!/usr/bin/env python3
import aws_cdk as cdk
from mursst_stack import MursstStack

app = cdk.App()
MursstStack(app, "MursstStack")

app.synth()
