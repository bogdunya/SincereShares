from django.db import models

# Create your models here.
from django.db import models

class Share(models.Model):
    ticker = models.CharField(max_length=40)
    name = models.CharField(max_length=255)
    slug = models.SlugField(unique=True, max_length=255)
    isin = models.CharField(max_length=12)


class Price(models.Model):
    share = models.ForeignKey(Share, on_delete=models.CASCADE)
    date = models.DateTimeField(auto_now_add=True)
    price = models.FloatField()
    volume = models.FloatField()
    change = models.FloatField()
    open = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()
    currency = models.CharField(max_length=10)

