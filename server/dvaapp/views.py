from django.conf import settings
import json
from .models import Video, Frame, DVAPQL, QueryResult, TEvent, IndexEntries, Region, Tube, Segment, \
    TubeRegionRelation, TubeRelation, Retriever, SystemState, QueryRegion, \
    TrainedModel, Worker, TrainingSet, RegionRelation, Export, HyperRegionRelation, HyperTubeRegionRelation, TaskRestart
import serializers
from rest_framework import viewsets
from django.contrib.auth.models import User
from rest_framework.permissions import IsAuthenticatedOrReadOnly, IsAuthenticated
from rest_framework.response import Response
from .processing import DVAPQLProcess
from dva.in_memory import redis_client
import logging

try:
    from django.contrib.postgres.search import SearchVector
except ImportError:
    SearchVector = None
    logging.warning("Could not load Postgres full text search")


def user_check(user):
    return user.is_authenticated or settings.AUTH_DISABLED


def force_user_check(user):
    return user.is_authenticated


class UserViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = User.objects.all()
    serializer_class = serializers.UserSerializer


class SystemStateViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = SystemState.objects.all()
    serializer_class = serializers.SystemStateSerializer


class VideoViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Video.objects.all()
    serializer_class = serializers.VideoSerializer


class ExportViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Export.objects.all()
    serializer_class = serializers.ExportSerializer


class RetrieverViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Retriever.objects.all()
    serializer_class = serializers.RetrieverSerializer


class TrainedModelViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TrainedModel.objects.all()
    serializer_class = serializers.TrainedModelSerializer


class TrainingSetViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TrainingSet.objects.all()
    serializer_class = serializers.TrainingSetSerializer


class FrameViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Frame.objects.all()
    serializer_class = serializers.FrameSerializer
    filter_fields = ('frame_index', 'name', 'video')


class SegmentViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Segment.objects.all()
    serializer_class = serializers.SegmentSerializer
    filter_fields = ('segment_index', 'video')


class QueryRegionViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = QueryRegion.objects.all()
    serializer_class = serializers.QueryRegionSerializer


class RegionViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Region.objects.all()
    serializer_class = serializers.RegionSerializer
    filter_fields = ('video',)


class DVAPQLViewSet(viewsets.ModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = DVAPQL.objects.all()
    serializer_class = serializers.DVAPQLSerializer

    def perform_create(self, serializer):
        instance = serializer.save(user=self.request.user)
        p = DVAPQLProcess(instance)
        spec = json.loads(self.request.POST.get('script'))
        p.create_from_json(spec, self.request.user)
        p.launch()

    def perform_update(self, serializer):
        """
        Immutable Not allowed
        :param serializer:
        :return:
        """
        raise ValueError("Not allowed to mutate")

    def perform_destroy(self, instance):
        """
        :param instance:
        :return:
        """
        raise ValueError("Not allowed to delete")


class QueryResultsViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = QueryResult.objects.all()
    serializer_class = serializers.QueryResultsSerializer
    filter_fields = ('query',)


class TEventViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TEvent.objects.all()
    serializer_class = serializers.TEventSerializer
    filter_fields = ('video', 'operation', 'completed', 'started', 'errored', 'parent_process')


class TaskRestartViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TaskRestart.objects.all()
    serializer_class = serializers.TaskRestartSerializer
    filter_fields = ('process','video_uuid')


class WorkerViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Worker.objects.all()
    serializer_class = serializers.WorkerSerializer


class IndexEntriesViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = IndexEntries.objects.all()
    serializer_class = serializers.IndexEntriesSerializer
    filter_fields = ('video', 'algorithm', 'target', 'indexer_shasum', 'approximator_shasum', 'approximate')


class TubeViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = Tube.objects.all()
    serializer_class = serializers.TubeSerializer


class RegionRelationViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = RegionRelation.objects.all()
    serializer_class = serializers.RegionRelationSerializer


class HyperRegionRelationViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = HyperRegionRelation.objects.all()
    serializer_class = serializers.HyperRegionRelationSerializer


class HyperTubeRegionRelationViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = HyperTubeRegionRelation.objects.all()
    serializer_class = serializers.HyperTubeRegionRelationSerializer


class TubeRelationViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TubeRelation.objects.all()
    serializer_class = serializers.TubeRelationSerializer


class TubeRegionRelationViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)
    queryset = TubeRegionRelation.objects.all()
    serializer_class = serializers.TubeRegionRelationSerializer


class RetrieverStateViewState(viewsets.ViewSet):
    permission_classes = (IsAuthenticatedOrReadOnly,) if settings.AUTH_DISABLED else (IsAuthenticated,)

    def list(self, request, format=None):
        """
        Returns state of the retriever
        """
        retriever_state = redis_client.hgetall("retriever_state")
        if retriever_state:
            return Response({k:json.loads(v) for k,v in retriever_state.items()})
        else:
            return Response({})
