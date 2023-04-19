from django.shortcuts import render
from django.http import HttpResponse

# Create your views here.

def index(request):
    return HttpResponse("<h1>Привет, это начало проекта SincereShares!</h1> "
                        "Здесь мы будем рисовать графики с финансовыми данными из MOEX ...")