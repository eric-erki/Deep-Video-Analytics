from django.shortcuts import render, redirect
from django.conf import settings
from collections import defaultdict
from django.http import JsonResponse
import json
from django.views.generic import ListView, DetailView
from .forms import UploadFileForm, YTVideoForm, AnnotationForm
from dvaapp import models
from .models import StoredDVAPQL, ExternalServer
from dva.celery import app
from dva.in_memory import redis_client
from datetime import datetime
from django.utils import timezone
import math
from django.db.models import Max
import view_shared
import debug_mode
from dvaapp.processing import DVAPQLProcess
from django.contrib.auth.decorators import user_passes_test, login_required
from django.utils.decorators import method_decorator
from django.contrib.auth.mixins import UserPassesTestMixin
from django_celery_results.models import TaskResult
from rest_framework.authtoken.models import Token
import logging

try:
    from django.contrib.postgres.search import SearchVector
except ImportError:
    SearchVector = None
    logging.warning("Could not load Postgres full text search")


class LoginRequiredMixin(object):
    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(LoginRequiredMixin, self).dispatch(*args, **kwargs)


def user_check(user):
    return user.is_authenticated or settings.AUTH_DISABLED


def force_user_check(user):
    return user.is_authenticated


class VideoList(UserPassesTestMixin, ListView):
    model = models.Video
    paginate_by = 100
    template_name = "dvaui/video_list.html"

    def get_context_data(self, **kwargs):
        context = super(VideoList, self).get_context_data(**kwargs)
        context['exports'] = models.Export.objects.all().filter(export_type=models.Export.VIDEO_EXPORT)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TEventDetail(UserPassesTestMixin, DetailView):
    model = models.TEvent
    template_name = "dvaui/tevent_detail.html"

    def get_context_data(self, **kwargs):
        context = super(TEventDetail, self).get_context_data(**kwargs)
        context['child_tasks'] = models.TEvent.objects.filter(parent_id=context['object'].pk)
        try:
            tr = TaskResult.objects.get(task_id=context['object'].task_id)
        except TaskResult.DoesNotExist:
            context['celery_task'] = None
            pass
        else:
            context['celery_task'] = tr
        return context

    def test_func(self):
        return user_check(self.request.user)


class TEventList(UserPassesTestMixin, ListView):
    model = models.TEvent
    paginate_by = 500
    template_name = "dvaui/tevent_list.html"

    def get_queryset(self):
        kwargs = {}
        if self.kwargs.get('pk', None):
            kwargs['video_id'] = self.kwargs['pk']
        elif self.kwargs.get('process_pk', None):
            kwargs['parent_process_id'] = self.kwargs['process_pk']
        if self.kwargs.get('status', None):
            if self.kwargs['status'] == 'running':
                kwargs['duration__lt'] = 0
                kwargs['started'] = True
                kwargs['completed'] = False
                kwargs['errored'] = False
            elif self.kwargs['status'] == 'successful':
                kwargs['completed'] = True
            elif self.kwargs['status'] == 'pending':
                kwargs['duration__lt'] = 0
                kwargs['started'] = False
                kwargs['errored'] = False
            elif self.kwargs['status'] == 'failed':
                kwargs['errored'] = True
        new_context = models.TEvent.objects.filter(**kwargs).order_by('-created').prefetch_related('video')
        return new_context

    def get_context_data(self, **kwargs):
        view_shared.refresh_task_status()
        context = super(TEventList, self).get_context_data(**kwargs)
        started_series = {}
        created_series = {}
        points = defaultdict(list)
        for k in context['object_list']:
            series_name = '{} on {}'.format(k.operation, k.queue)
            if k.start_ts:
                if series_name not in started_series:
                    started_series[series_name] = {'name':series_name, 'type': "scatter",'x':[],'y':[],"mode":"markers"}
                started_series[series_name]['x'].append(str(k.start_ts))
                started_series[series_name]['y'].append(k.duration)
            if series_name not in created_series:
                created_series[series_name] = {'name':series_name, 'type': "scatter",'x':[],'y':[],"mode":"markers"}
            created_series[series_name]['x'].append(str(k.created))
            created_series[series_name]['y'].append(k.duration)
        context['start_plot_data'] = json.dumps(started_series.values())
        context['created_plot_data'] = json.dumps(created_series.values())
        context['header'] = "Across all processes"
        if self.kwargs.get('pk', None):
            context['video'] = models.Video.objects.get(pk=self.kwargs['pk'])
            context['header'] = "video/dataset : {}".format(context['video'].name)
        if self.kwargs.get('process_pk', None):
            process_pk = self.kwargs.get('process_pk', None)
            context['header'] = "process : {}".format(process_pk)
        if self.kwargs.get('status', None):
            context['header'] += " with status {}".format(self.kwargs['status'])
        return context

    def test_func(self):
        return user_check(self.request.user)


class VideoDetail(UserPassesTestMixin, DetailView):
    model = models.Video
    template_name = "dvaui/video_detail.html"

    def get_context_data(self, **kwargs):
        context = super(VideoDetail, self).get_context_data(**kwargs)
        max_frame_index = models.Frame.objects.all().filter(video=self.object).aggregate(Max('frame_index'))[
            'frame_index__max']
        context['exports'] = models.TEvent.objects.all().filter(operation='perform_export', video=self.object)
        context['annotation_count'] = models.Region.objects.all().filter(video=self.object,
                                                                  region_type=models.Region.ANNOTATION).count()
        context['exportable_annotation_count'] = 0
        context['url'] = '{}{}/video/{}.mp4'.format(settings.MEDIA_URL, self.object.pk, self.object.pk)
        label_list = []
        context['label_list'] = label_list
        delta = 5000
        if context['object'].dataset:
            delta = 500
        if max_frame_index <= delta:
            context['frame_list'] = models.Frame.objects.all().filter(video=self.object).order_by('frame_index')
            context['offset'] = 0
            context['limit'] = max_frame_index
        else:
            if self.request.GET.get('frame_index_offset', None) is None:
                offset = 0
            else:
                offset = int(self.request.GET.get('frame_index_offset'))
            limit = offset + delta
            context['offset'] = offset
            context['limit'] = limit
            context['frame_list'] = models.Frame.objects.all().filter(video=self.object, frame_index__gte=offset,
                                                               frame_index__lte=limit).order_by('frame_index')
            context['frame_index_offsets'] = [(k * delta, (k * delta) + delta) for k in
                                              range(int(math.ceil(max_frame_index / float(delta))))]
        context['frame_first'] = context['frame_list'].first()
        context['frame_last'] = context['frame_list'].last()
        context['task_list'] = models.TEvent.objects.all().filter(video=self.object)
        context['segments'] = models.Segment.objects.filter(video=self.object)
        context['pending_tasks'] = models.TEvent.objects.all().filter(video=self.object, started=False, errored=False).count()
        context['running_tasks'] = models.TEvent.objects.all().filter(video=self.object, started=True, completed=False,
                                                               errored=False).count()
        context['successful_tasks'] = models.TEvent.objects.all().filter(video=self.object, completed=True).count()
        context['errored_tasks'] = models.TEvent.objects.all().filter(video=self.object, errored=True).count()
        if context['limit'] > max_frame_index:
            context['limit'] = max_frame_index
        context['max_frame_index'] = max_frame_index
        return context

    def test_func(self):
        return user_check(self.request.user)


class FrameDetail(UserPassesTestMixin, DetailView):
    model = models.Frame
    template_name = 'dvaui/frame_detail.html'

    def get_context_data(self, **kwargs):
        context = super(FrameDetail, self).get_context_data(**kwargs)
        context['detection_list'] = models.Region.objects.all().filter(frame_index=self.object.frame_index,
                                                                video_id=self.object.video_id,
                                                                region_type=models.Region.DETECTION)
        context['annotation_list'] = models.Region.objects.all().filter(frame_index=self.object.frame_index,
                                                                 video_id=self.object.video_id,
                                                                 region_type=models.Region.ANNOTATION)
        context['video'] = self.object.video
        context['url'] = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, self.object.video.pk, self.object.frame_index)
        context['previous_frame'] = models.Frame.objects.filter(video=self.object.video,
                                                         frame_index__lt=self.object.frame_index).order_by(
            '-frame_index')[0:1]
        context['next_frame'] = models.Frame.objects.filter(video=self.object.video,
                                                            frame_index__gt=self.object.frame_index).order_by('frame_index')[
                                0:1]
        return context

    def test_func(self):
        return user_check(self.request.user)


class RegionDetail(UserPassesTestMixin, DetailView):
    model = models.Region
    template_name = 'dvaui/region_detail.html'

    def get_context_data(self, **kwargs):
        context = super(RegionDetail, self).get_context_data(**kwargs)
        context['video'] = self.object.video
        context['url'] = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, self.object.video.pk, self.object.frame_index)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TubeDetail(UserPassesTestMixin, DetailView):
    model = models.Tube
    template_name = 'dvaui/tube_detail.html'

    def get_context_data(self, **kwargs):
        context = super(TubeDetail, self).get_context_data(**kwargs)
        context['video'] = self.object.video
        return context

    def test_func(self):
        return user_check(self.request.user)


class SegmentDetail(UserPassesTestMixin, DetailView):
    model = models.Segment
    template_name = 'dvaui/segment_detail.html'

    def get_context_data(self, **kwargs):
        context = super(SegmentDetail, self).get_context_data(**kwargs)
        context['video'] = self.object.video
        context['frame_list'] = models.Frame.objects.all().filter(video=self.object.video,
                                                           segment_index=self.object.segment_index).order_by(
            'frame_index')
        context['region_list'] = models.Region.objects.all().filter(video=self.object.video,
                                                             segment_index=self.object.segment_index).order_by(
            'frame_index')
        context['url'] = '{}{}/segments/{}.mp4'.format(settings.MEDIA_URL, self.object.video.pk,
                                                       self.object.segment_index)
        context['previous_segment_index'] = self.object.segment_index - 1 if self.object.segment_index else None
        if (self.object.segment_index + 1) < self.object.video.segments:
            context['next_segment_index'] = self.object.segment_index + 1
        else:
            context['next_segment_index'] = None
        return context

    def test_func(self):
        return user_check(self.request.user)


class VisualSearchList(UserPassesTestMixin, ListView):
    model = models.DVAPQL
    template_name = "dvaui/query_list.html"

    def test_func(self):
        return user_check(self.request.user)

    def get_queryset(self):
        new_context = models.DVAPQL.objects.filter(process_type=models.DVAPQL.QUERY).order_by('-created')
        return new_context


class VisualSearchDetail(UserPassesTestMixin, DetailView):
    model = models.DVAPQL
    template_name = "dvaui/query_detail.html"

    def get_context_data(self, **kwargs):
        context = super(VisualSearchDetail, self).get_context_data(**kwargs)
        qp = DVAPQLProcess(process=context['object'], media_dir=settings.MEDIA_ROOT)
        qp_context = view_shared.collect(qp)
        context['results'] = qp_context['results'].items()
        context['regions'] = []
        for k in qp_context['regions']:
            if 'results' in k and k['results']:
                k['results'] = k['results'].items()
            context['regions'].append(k)
        script = context['object'].script
        script[u'image_data_b64'] = "<excluded>"
        context['plan'] = script
        context['pending_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, started=False,
                                                               errored=False).count()
        context['running_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, started=True,
                                                               completed=False, errored=False).count()
        context['successful_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object,
                                                                         completed=True).count()
        context['errored_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, errored=True).count()
        context['url'] = '{}queries/{}.png'.format(settings.MEDIA_URL, self.object.uuid)
        return context

    def test_func(self):
        return user_check(self.request.user)


class ProcessList(UserPassesTestMixin, ListView):
    model = models.DVAPQL
    template_name = "dvaui/process_list.html"
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super(ProcessList, self).get_context_data(**kwargs)
        return context

    def test_func(self):
        return user_check(self.request.user)

    def get_queryset(self):
        new_context = models.DVAPQL.objects.filter().order_by('-created')
        return new_context


class RetrieverList(UserPassesTestMixin, ListView):
    model = models.Retriever
    template_name = "dvaui/retriever_list.html"
    paginate_by = 100

    def get_context_data(self, **kwargs):
        context = super(RetrieverList, self).get_context_data(**kwargs)
        retriever_state = redis_client.hgetall("retriever_state")
        if retriever_state:
            context['retriever_state'] = [json.loads(v) for k,v in retriever_state.items()]
        else:
            context['retriever_state'] = []
        for k in context['retriever_state']:
            k['ts'] = datetime.fromtimestamp(k['ts'],tz=timezone.utc)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TrainedModelList(UserPassesTestMixin, ListView):
    model = models.TrainedModel
    template_name = "dvaui/model_list.html"
    paginate_by = 100

    def get_context_data(self, **kwargs):
        context = super(TrainedModelList, self).get_context_data(**kwargs)
        context['exports'] = models.Export.objects.filter(export_type=models.Export.MODEL_EXPORT)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TrainedModelDetail(UserPassesTestMixin, DetailView):
    model = models.TrainedModel
    template_name = "dvaui/model_detail.html"

    def get_context_data(self, **kwargs):
        context = super(TrainedModelDetail, self).get_context_data(**kwargs)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TrainingSetList(UserPassesTestMixin, ListView):
    model = models.TrainingSet
    template_name = "dvaui/training_set_list.html"
    paginate_by = 50

    class Meta:
        ordering = ["-created"]

    def get_context_data(self, **kwargs):
        context = super(TrainingSetList, self).get_context_data(**kwargs)
        return context

    def test_func(self):
        return user_check(self.request.user)


class TrainingSetDetail(UserPassesTestMixin, DetailView):
    model = models.TrainingSet
    template_name = "dvaui/training_set_detail.html"

    def get_context_data(self, **kwargs):
        context = super(TrainingSetDetail, self).get_context_data(**kwargs)
        context['trained_model_set'] = models.TrainedModel.objects.filter(training_set=context['object'])
        return context

    def test_func(self):
        return user_check(self.request.user)


class IndexEntryList(UserPassesTestMixin, ListView):
    model = models.IndexEntries
    template_name = "dvaui/index_list.html"
    paginate_by = 100

    def get_context_data(self, **kwargs):
        context = super(IndexEntryList, self).get_context_data(**kwargs)
        return context

    def test_func(self):
        return user_check(self.request.user)


class ProcessDetail(UserPassesTestMixin, DetailView):
    model = models.DVAPQL
    template_name = "dvaui/process_detail.html"

    def get_context_data(self, **kwargs):
        context = super(ProcessDetail, self).get_context_data(**kwargs)
        context['json'] = json.dumps(context['object'].script, indent=4)
        context['pending_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, started=False,
                                                               errored=False).count()
        context['running_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, started=True,
                                                               completed=False, errored=False).count()
        context['successful_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, completed=True).count()
        context['errored_tasks'] = models.TEvent.objects.all().filter(parent_process=self.object, errored=True).count()
        return context

    def test_func(self):
        return user_check(self.request.user)


class StoredProcessList(UserPassesTestMixin, ListView):
    model = StoredDVAPQL
    template_name = "dvaui/stored_process_list.html"
    paginate_by = 500
    ordering = "-created"

    def get_context_data(self, **kwargs):
        context = super(StoredProcessList, self).get_context_data(**kwargs)
        context['indexers'] = models.TrainedModel.objects.filter(model_type=models.TrainedModel.INDEXER)
        context['approximators'] = models.TrainedModel.objects.filter(model_type=models.TrainedModel.APPROXIMATOR)
        context['models'] = models.TrainedModel.objects.filter(model_type__in=[models.TrainedModel.INDEXER, models.TrainedModel.DETECTOR,
                                                                               models.TrainedModel.ANALYZER])
        context["videos"] = models.Video.objects.all()
        context["approx_training_sets"] = models.TrainingSet.objects.filter(training_task_type=models.TrainingSet.TRAINAPPROX,
                                                                   built=True)
        return context

    def test_func(self):
        return user_check(self.request.user)


class StoredProcessDetail(UserPassesTestMixin, DetailView):
    model = StoredDVAPQL
    template_name = "dvaui/stored_process_detail.html"

    def get_context_data(self, **kwargs):
        context = super(StoredProcessDetail, self).get_context_data(**kwargs)
        context['json'] = json.dumps(context['object'].script, indent=4)
        return context

    def test_func(self):
        return user_check(self.request.user)


@user_passes_test(user_check)
def search(request):
    if request.method == 'POST':
        qp = DVAPQLProcess()
        view_shared.create_query_from_request(qp, request)
        qp.launch()
        qp.wait_query()
        return JsonResponse(data={'url': '/queries/{}/'.format(qp.process.pk)})
    else:
        raise ValueError("Only POST method is valid")


@user_passes_test(user_check)
def debug(request):
    if settings.DEBUG:
        return render(request, 'dvaui/debug.html', {
            'workers':debug_mode.list_workers()
        })
    else:
        return redirect('app_home')


@user_passes_test(user_check)
def debug_restart_workers(request):
    if settings.DEBUG:
        if request.method == 'POST':
            debug_mode.restart_all_workers()
    return redirect('debug')


@user_passes_test(user_check)
def index(request, query_pk=None, frame_pk=None, detection_pk=None):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        user = request.user if request.user.is_authenticated else None
        if form.is_valid():
            view_shared.handle_uploaded_file(request.FILES['file'], form.cleaned_data['name'], user=user)
            return redirect('video_list')
        else:
            raise ValueError
    else:
        form = UploadFileForm()
    context = {'form': form,
               'detectors': models.TrainedModel.objects.filter(model_type=models.TrainedModel.DETECTOR),
               'indexer_retrievers': []}
    for i in models.TrainedModel.objects.filter(model_type=models.TrainedModel.INDEXER):
        for r in models.Retriever.objects.all():
            if i.shasum and r.indexer_shasum == i.shasum:
                context['indexer_retrievers'].append(('{} > {} retriever {} (pk:{})'.format(i.name,
                                                                                            r.get_algorithm_display(),
                                                                                            r.name, r.pk),
                                                      '{}_{}'.format(i.pk, r.pk)))
    if query_pk:
        previous_query = models.DVAPQL.objects.get(pk=query_pk)
        context['initial_url'] = '{}queries/{}.png'.format(settings.MEDIA_URL, previous_query.uuid)
    elif frame_pk:
        frame = models.Frame.objects.get(pk=frame_pk)
        context['initial_url'] = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, frame.video.pk, frame.frame_index)
    elif detection_pk:
        detection = models.Region.objects.get(pk=detection_pk)
        context['initial_url'] = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, detection.video.pk,
                                                             detection.frame_index)
    context['frame_count'] = models.Frame.objects.count()
    context['query_count'] = models.DVAPQL.objects.filter(process_type=models.DVAPQL.QUERY).count()
    context['process_count'] = models.DVAPQL.objects.filter(process_type=models.DVAPQL.PROCESS).count()
    context['restart_count'] = models.TaskRestart.objects.filter().count()
    context['index_entries_count'] = models.IndexEntries.objects.count()
    context['task_events_count'] = models.TEvent.objects.count()
    context['pending_tasks'] = models.TEvent.objects.all().filter(started=False, errored=False).count()
    context['running_tasks'] = models.TEvent.objects.all().filter(started=True, completed=False, errored=False).count()
    context['successful_tasks'] = models.TEvent.objects.all().filter(started=True, completed=True).count()
    context['errored_tasks'] = models.TEvent.objects.all().filter(errored=True).count()
    context['video_count'] = models.Video.objects.count()
    context['index_entries'] = models.IndexEntries.objects.all()
    context['region_count'] = models.Region.objects.all().count()
    context['models_count'] = models.TrainedModel.objects.all().count()
    context['worker_count'] = models.Worker.objects.all().count()
    context['training_set_count'] = models.TrainingSet.objects.all().count()
    context['retriever_counts'] = models.Retriever.objects.all().count()
    context['external_server_count'] = ExternalServer.objects.all().count()
    context['script_count'] = StoredDVAPQL.objects.all().count()
    context['tube_count'] = models.Tube.objects.all().count()
    context["videos"] = models.Video.objects.all().filter()
    context["exported_video_count"] = models.Export.objects.filter(export_type=models.Export.VIDEO_EXPORT).count()
    context["exported_model_count"] = models.Export.objects.filter(export_type=models.Export.MODEL_EXPORT).count()
    context['rate'] = settings.DEFAULT_RATE
    return render(request, 'dvaui/dashboard.html', context)


@user_passes_test(user_check)
def annotate(request, frame_pk):
    context = {'frame': None, 'detection': None, 'existing': []}
    frame = models.Frame.objects.get(pk=frame_pk)
    context['frame'] = frame
    context['initial_url'] = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, frame.video.pk, frame.frame_index)
    context['previous_frame'] = models.Frame.objects.filter(video=frame.video, frame_index__lt=frame.frame_index).order_by(
        '-frame_index')[0:1]
    context['next_frame'] = models.Frame.objects.filter(video=frame.video, frame_index__gt=frame.frame_index).order_by(
        'frame_index')[0:1]
    context['detections'] = models.Region.objects.filter(video=frame.video, frame_index=frame.frame_index,
                                                  region_type=models.Region.DETECTION)
    for d in models.Region.objects.filter(video=frame.video, frame_index=frame.frame_index):
        temp = {
            'x': d.x,
            'y': d.y,
            'h': d.h,
            'w': d.w,
            'pk': d.pk,
            'box_type': "detection" if d.region_type == d.DETECTION else 'annotation',
            'label': d.object_name,
            'full_frame': d.full_frame,
            'detection_pk': None
        }
        context['existing'].append(temp)
    context['existing'] = json.dumps(context['existing'])
    if request.method == 'POST':
        form = AnnotationForm(request.POST)
        if form.is_valid():
            applied_tags = form.cleaned_data['tags'].split(',') if form.cleaned_data['tags'] else []
            view_shared.create_annotation(form, form.cleaned_data['object_name'], applied_tags, frame,
                                          user=request.user if request.user.is_authenticated else None)
            return JsonResponse({'status': True})
        else:
            raise ValueError(form.errors)
    return render(request, 'dvaui/annotate.html', context)


@user_passes_test(user_check)
def yt(request):
    if request.method == 'POST':
        form = YTVideoForm(request.POST, request.FILES)
        user = request.user if request.user.is_authenticated else None
        if form.is_valid():
            name = form.cleaned_data['name']
            path = form.cleaned_data['url']
            process_spec = {
                'process_type': models.DVAPQL.PROCESS,
                'create': [
                    {'spec':
                        {
                            'name': name,
                            'uploader_id': user.pk if user else None,
                            'url': path,
                            'created': '__timezone.now__'
                        },
                        'MODEL': 'Video',
                    },
                ],
                'map': [
                    {'video_id': '__created__0',
                     'operation': 'perform_import',
                     'arguments': {
                         'force_youtube_dl': True,
                         'map': [{
                             'operation': 'perform_video_segmentation',
                             'arguments': {
                                 'map': [
                                     {'operation': 'perform_video_decode',
                                      'arguments': {
                                          'rate': settings.DEFAULT_RATE,
                                          'segments_batch_size': settings.DEFAULT_SEGMENTS_BATCH_SIZE,
                                          'map': view_shared.DEFAULT_PROCESSING['video']
                                      }
                                      }
                                 ]},
                         }, ]
                     }
                     },
                ]
            }
            p = DVAPQLProcess()
            p.create_from_json(process_spec, user)
            p.launch()
        else:
            raise ValueError
    else:
        raise NotImplementedError
    return redirect('video_list')


@user_passes_test(user_check)
def segment_by_index(request, pk, segment_index):
    segment = models.Segment.objects.get(video_id=pk, segment_index=segment_index)
    return redirect('segment_detail', pk=segment.pk)


@user_passes_test(user_check)
def frame_by_index(request, pk, frame_index):
    try:
        df = models.Frame.objects.get(video_id=pk, frame_index=frame_index)
    except:
        df = None
        pass
    if df:
        return redirect('frame_detail', pk=df.pk)
    else:
        # If the frame has not been decoded return the nearest segment
        segment = models.Segment.objects.filter(video_id=pk, start_index__lte=frame_index).order_by('-start_index').first()
        return redirect('segment_detail', pk=segment.pk)


@user_passes_test(user_check)
def export_video(request):
    if request.method == 'POST':
        pk = request.POST.get('video_id')
        video = models.Video.objects.get(pk=pk)
        export_method = request.POST.get('export_method')
        if video:
            if export_method == 's3':
                path = request.POST.get('path')
                process_spec = {'process_type': models.DVAPQL.PROCESS,
                                'map': [
                                    {
                                        'operation': 'perform_export',
                                        'arguments': {'path': path, 'video_selector': {'pk': video.pk}, }
                                    },
                                ]}
            else:
                process_spec = {'process_type': models.DVAPQL.PROCESS,
                                'map': [
                                    {
                                        'operation': 'perform_export',
                                        'arguments': {'video_selector': {'pk': video.pk}, }
                                    },
                                ]
                                }
            p = DVAPQLProcess()
            p.create_from_json(process_spec)
            p.launch()
        return redirect('video_list')
    else:
        raise NotImplementedError


@user_passes_test(user_check)
def management(request):
    timeout = 1.0
    context = {
        'timeout': timeout,
        'actions': models.ManagementAction.objects.all(),
        'workers': models.Worker.objects.all(),
        'restarts': models.TaskRestart.objects.all(),
        'state': models.SystemState.objects.all().order_by('-created')[:100]
    }
    if request.method == 'POST':
        op = request.POST.get("op", "")
        if op == "list":
            t = app.send_task('manage_host', args=[], exchange='qmanager')
            t.wait(timeout=timeout)
    return render(request, 'dvaui/management.html', context)


@user_passes_test(user_check)
def textsearch(request):
    context = {'results': {}, "videos": models.Video.objects.all().filter()}
    q = request.GET.get('q')
    if q:
        offset = int(request.GET.get('offset', 0))
        delta = int(request.GET.get('delta', 25))
        limit = offset + delta
        context['q'] = q
        context['next'] = limit
        context['delta'] = delta
        context['offset'] = offset
        context['limit'] = limit
        if request.GET.get('regions'):
            context['results']['regions_meta'] = models.Region.objects.filter(text__search=q)[offset:limit]
            context['results']['regions_name'] = models.Region.objects.filter(object_name__search=q)[offset:limit]
        if request.GET.get('frames'):
            context['results']['frames_name'] = models.Frame.objects.filter(name__search=q)[offset:limit]
    return render(request, 'dvaui/textsearch.html', context)


@user_passes_test(user_check)
def submit_process(request):
    if request.method == 'POST':
        process_pk = request.POST.get('process_pk', None)
        if process_pk is None:
            p = DVAPQLProcess()
            p.create_from_json(j=json.loads(request.POST.get('script')),
                               user=request.user if request.user.is_authenticated else None)
            p.launch()
        else:
            p = DVAPQLProcess(process=models.DVAPQL.objects.get(pk=process_pk))
            p.launch()
        return redirect("process_detail", pk=p.process.pk)


@user_passes_test(user_check)
def validate_process(request):
    if request.method == 'POST':
        p = DVAPQLProcess()
        p.create_from_json(j=json.loads(request.POST.get('script')),
                           user=request.user if request.user.is_authenticated else None)
        p.validate()
        return redirect("process_detail", pk=p.process.pk)
    else:
        raise ValueError("Request must be a POST")


@user_passes_test(force_user_check)
def security(request):
    context = {'username': request.user.username}
    token, created = Token.objects.get_or_create(user=request.user if request.user.is_authenticated else None)
    context['token'] = token
    return render(request, 'dvaui/security.html', context=context)


@user_passes_test(force_user_check)
def expire_token(request):
    # TODO Check if this is correct
    if request.method == 'POST':
        if request.POST.get('expire', False):
            token, created = Token.objects.get_or_create(user=request.user if request.user.is_authenticated else None)
            if not created:
                token.delete()
    return redirect("security")


@user_passes_test(user_check)
def import_s3(request):
    if request.method == 'POST':
        keys = request.POST.get('key')
        user = request.user if request.user.is_authenticated else None
        create = []
        map_tasks = []
        counter = 0
        for key in keys.strip().split('\n'):
            dataset_type = False
            key = key.strip()
            if key:
                extract_task = {
                    'arguments': {'map': view_shared.DEFAULT_PROCESSING['dataset']},
                    'operation': 'perform_dataset_extraction'}
                segment_decode_task = {'operation': 'perform_video_segmentation',
                                       'arguments': {
                                           'map': [
                                               {'operation': 'perform_video_decode',
                                                'arguments': {
                                                    'segments_batch_size': settings.DEFAULT_SEGMENTS_BATCH_SIZE,
                                                    'map': view_shared.DEFAULT_PROCESSING['video']
                                                }
                                                }
                                           ]},
                                       }
                if key.endswith('.dva_export'):
                    next_tasks = []
                elif key.endswith('.zip'):
                    next_tasks = [extract_task, ]
                    dataset_type = True
                else:
                    next_tasks = [segment_decode_task, ]
                map_tasks.append({'video_id': '__created__{}'.format(counter),
                                  'operation': 'perform_import',
                                  'arguments': {
                                      'source': 'REMOTE',
                                      'map': next_tasks}
                                  })
                create.append({'MODEL': 'Video',
                               'spec': {'uploader_id': user.pk if user else None, 'dataset': dataset_type,
                                        'name': key, 'url': key},
                               })
                counter += 1
        process_spec = {'process_type': models.DVAPQL.PROCESS,
                        'create': create,
                        'map': map_tasks
                        }
        p = DVAPQLProcess()
        p.create_from_json(process_spec, user)
        p.launch()
    else:
        raise NotImplementedError
    return redirect('video_list')


@user_passes_test(user_check)
def pull_external(request):
    if request.method == 'POST':
        server_pk = request.POST.get('server_pk', None)
        s = ExternalServer.objects.get(pk=server_pk)
        s.pull()
    return redirect('external')


@user_passes_test(user_check)
def external(request):
    context = {
        'servers': ExternalServer.objects.all(),
        'scripts': StoredDVAPQL.objects.all(),
    }
    return render(request, 'dvaui/external_data.html', context)


@user_passes_test(user_check)
def retry_task(request):
    pk = request.POST.get('pk')
    event = models.TEvent.objects.get(pk=int(pk))
    spec = {
        'process_type': models.DVAPQL.PROCESS,
        'map': [
            {
                'operation': event.operation,
                'arguments': event.arguments
            }
        ]
    }
    p = DVAPQLProcess()
    p.create_from_json(spec)
    p.launch()
    return redirect('/processes/')


@user_passes_test(user_check)
def delete_video(request):
    if request.user.is_staff:  # currently only staff can delete
        video_pk = request.POST.get('video_id')
        view_shared.delete_video_object(video_pk, request.user)
        return redirect('video_list')
    else:
        return redirect('accounts/login/')


@user_passes_test(user_check)
def shortcuts(request):
    user = request.user if request.user.is_authenticated else None
    if request.method == 'POST':
        if request.POST.get('op') == 'apply':
            jf = request.POST.get("filters", '{}')
            filters = json.loads(jf) if jf.strip() else {}
            model_pk = request.POST.get("model_pk")
            video_pks = request.POST.getlist('video_pk')
            target = request.POST.get('target')
            frames_batch_size = request.POST.get('frames_batch_size', )
            if not frames_batch_size:
                frames_batch_size = settings.DEFAULT_FRAMES_BATCH_SIZE
            segments_batch_size = request.POST.get('segments_batch_size')
            if not segments_batch_size:
                segments_batch_size = settings.DEFAULT_SEGMENTS_BATCH_SIZE
            process_pk = view_shared.model_apply(model_pk, video_pks, filters, target, int(segments_batch_size),
                                                 int(frames_batch_size), user)
            return redirect('process_detail', pk=process_pk)
        elif request.POST.get('op') == 'create_retriever':
            jf = request.POST.get("source_filters", '{}')
            filters = json.loads(jf) if jf.strip() else {}
            name = request.POST.get('name')
            indexer_shasum = request.POST.get('indexer_shasum')
            approximator_shasum = request.POST.get('approximator_shasum')
            if approximator_shasum:
                approximator_shasum = None
                algorithm = models.Retriever.LOPQ
            else:
                algorithm = models.Retriever.EXACT
            _ = view_shared.create_retriever(name, algorithm, filters, indexer_shasum, approximator_shasum, user)
            return redirect('retriever_list')
        elif request.POST.get('op') == 'create_approximator_training_set':
            name = request.POST.get('name')
            video_pks = request.POST.getlist('video_pk')
            indexer_shasum = request.POST.get('indexer_shasum')
            _ = view_shared.create_approximator_training_set(name, indexer_shasum, video_pks, user)
            return redirect('training_set_list')
        elif request.POST.get('op') == 'perform_approximator_training':
            training_set_pk = request.POST.get('lopq_training_set_pk')
            dt = models.TrainingSet.objects.get(pk=training_set_pk)
            args = {'trainer': "LOPQ",
                    'name': request.POST.get('name'),
                    'indexer_shasum': dt.source_filters['indexer_shasum'],
                    'components': request.POST.get('components'),
                    'm': request.POST.get('m'),
                    'v': request.POST.get('v'),
                    'sub': request.POST.get('sub')}
            process_pk = view_shared.perform_training(training_set_pk, args, user)
            return redirect('process_detail', process_pk)
        elif request.POST.get('op') == 'export_model':
            process_pk = view_shared.perform_model_export(request.POST.get('model_pk'), user)
            return redirect('process_detail', process_pk)
        else:
            raise NotImplementedError(request.POST.get('op'))
    else:
        raise NotImplementedError("Only POST allowed")
