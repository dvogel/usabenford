
from django.shortcuts import render_to_response
from pymongo import Connection

Analyses = Connection().benford1.monthly_analyses

def agency(request, agency):
    analyses = Analyses.find({'agency': agency})
    return render_to_response('agency.html',
                              { 'agency': agency,
                                'analyses': analyses })

