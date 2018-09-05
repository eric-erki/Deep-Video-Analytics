import views
from rest_framework import routers
from django.conf.urls import url, include

router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'videos', views.VideoViewSet)
router.register(r'exports', views.ExportViewSet)
router.register(r'models', views.TrainedModelViewSet)
router.register(r'trainingset', views.TrainingSetViewSet)
router.register(r'retrievers', views.RetrieverViewSet)
router.register(r'tubes', views.TubeViewSet)
router.register(r'frames', views.FrameViewSet)
router.register(r'segments', views.SegmentViewSet)
router.register(r'regions', views.RegionViewSet)
router.register(r'regionrelations', views.RegionRelationViewSet)
router.register(r'hyperregionrelations', views.HyperRegionRelationViewSet)
router.register(r'tuberelations', views.TubeRelationViewSet)
router.register(r'hypertuberelations', views.HyperTubeRegionRelationViewSet)
router.register(r'tuberegionrelations', views.TubeRegionRelationViewSet)
router.register(r'queries', views.DVAPQLViewSet)
router.register(r'queryresults', views.QueryResultsViewSet)
router.register(r'queryregions', views.QueryRegionViewSet)
router.register(r'indexentries', views.IndexEntriesViewSet)
router.register(r'events', views.TEventViewSet)
router.register(r'restarts', views.TaskRestartViewSet)
router.register(r'workers', views.WorkerViewSet)
router.register(r'system_state', views.SystemStateViewSet)
router.register(r'retriever_state', views.RetrieverStateViewState, base_name='retriever_state')

urlpatterns = [url(r'', include(router.urls)), ]
