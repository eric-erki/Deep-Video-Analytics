from django.contrib import admin
from .models import Video, Frame, TEvent, IndexEntries, QueryResult, DVAPQL, Region, Tube, Segment, DeletedVideo, \
    ManagementAction, TrainedModel, Retriever, SystemState, Worker, QueryRegion, TrainingSet, Export, TaskRestart, \
    RegionRelation, TubeRelation, TubeRegionRelation, HyperRegionRelation, HyperTubeRegionRelation


@admin.register(HyperRegionRelation)
class HyperRegionRelationAdmin(admin.ModelAdmin):
    pass


@admin.register(HyperTubeRegionRelation)
class HyperTubeRegionRelationAdmin(admin.ModelAdmin):
    pass


@admin.register(RegionRelation)
class RegionRelationAdmin(admin.ModelAdmin):
    pass


@admin.register(TubeRelation)
class TubeRelationAdmin(admin.ModelAdmin):
    pass


@admin.register(TubeRegionRelation)
class TubeRegionRelationAdmin(admin.ModelAdmin):
    pass


@admin.register(TaskRestart)
class TaskRestartAdmin(admin.ModelAdmin):
    pass


@admin.register(Export)
class ExportAdmin(admin.ModelAdmin):
    pass


@admin.register(SystemState)
class SystemStateAdmin(admin.ModelAdmin):
    pass


@admin.register(Worker)
class WorkerAdmin(admin.ModelAdmin):
    pass


@admin.register(Segment)
class SegmentAdmin(admin.ModelAdmin):
    pass


@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    pass


@admin.register(Video)
class VideoAdmin(admin.ModelAdmin):
    pass


@admin.register(DeletedVideo)
class DeletedVideoAdmin(admin.ModelAdmin):
    pass


@admin.register(QueryResult)
class QueryResultsAdmin(admin.ModelAdmin):
    pass


@admin.register(DVAPQL)
class DVAPQLAdmin(admin.ModelAdmin):
    pass


@admin.register(Frame)
class FrameAdmin(admin.ModelAdmin):
    pass


@admin.register(IndexEntries)
class IndexEntriesAdmin(admin.ModelAdmin):
    pass


@admin.register(TEvent)
class TEventAdmin(admin.ModelAdmin):
    pass


@admin.register(Tube)
class TubeAdmin(admin.ModelAdmin):
    pass


@admin.register(TrainedModel)
class TrainedModelAdmin(admin.ModelAdmin):
    pass


@admin.register(Retriever)
class RetrieverAdmin(admin.ModelAdmin):
    pass


@admin.register(ManagementAction)
class ManagementActionAdmin(admin.ModelAdmin):
    pass


@admin.register(QueryRegion)
class QueryRegionAdmin(admin.ModelAdmin):
    pass


@admin.register(TrainingSet)
class TrainingSetAdmin(admin.ModelAdmin):
    pass
