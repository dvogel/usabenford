
from django.shortcuts import render
from pymongo import Connection, ASCENDING, DESCENDING

Analyses = Connection().benford1.monthly_analyses

def agency(request, agency):
    analyses = Analyses.find({'agency': agency}).sort([("year", ASCENDING), ("month", ASCENDING)] )
    return render(request, 'agency.html',
                              { 'agency': agency,
                                'analyses': analyses })

