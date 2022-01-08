from flask import Blueprint, request, jsonify
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from datetime import timedelta
from flask_cors import CORS, cross_origin
from sending_Class import mongo_operation
from data_pre import df_preprocessing, writer_dst
from member_detail import member_data_insertion

auth = Blueprint('auth', __name__)

 
