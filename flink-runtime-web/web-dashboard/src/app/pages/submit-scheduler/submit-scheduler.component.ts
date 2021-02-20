import { HttpEventType } from '@angular/common/http';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { Router } from '@angular/router';
import { JarFilesItemInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { takeUntil, flatMap } from 'rxjs/operators';
import { JarService, StatusService } from 'services';
import { DagreComponent } from 'share/common/dagre/dagre.component';

@Component({
  selector: 'flink-submit',
  templateUrl: './submit-scheduler.component.html',
  styleUrls: ['./submit-scheduler.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SubmitSchedulerComponent implements OnInit, OnDestroy {
  @ViewChild(DagreComponent) dagreComponent: DagreComponent;
  expandedMap = new Map();
  isLoading = true;
  destroy$ = new Subject();
  listOfSchedulers: JarFilesItemInterface[] = [];
  address: string;
  isYarn = false;
  noAccess = false;
  isUploading = false;
  progress = 0;
  validateForm: FormGroup;
  planVisible = false;

  uploadScheduler(file: File) {
    console.log(this.router.url);
    this.jarService.uploadScheduler(file).subscribe(
      event => {
        if (event.type === HttpEventType.UploadProgress && event.total) {
          this.isUploading = true;
          this.progress = Math.round((100 * event.loaded) / event.total);
        } else if (event.type === HttpEventType.Response) {
          this.isUploading = false;
          this.statusService.forceRefresh();
        }
      },
      () => {
        this.isUploading = false;
        this.progress = 0;
      }
    );
  }

  /**
   * trackBy Func
   * @param _
   * @param node
   */
  trackJarBy(_: number, node: JarFilesItemInterface) {
    return node.id;
  }

  constructor(
    private jarService: JarService,
    private statusService: StatusService,
    private fb: FormBuilder,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    this.isYarn = window.location.href.indexOf('/proxy/application_') !== -1;
    this.validateForm = this.fb.group({
      entryClass: [null],
      parallelism: [null],
      programArgs: [null],
      savepointPath: [null],
      allowNonRestoredState: [null]
    });
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.jarService.loadSchedulerList())
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.listOfSchedulers = data.files;
          this.address = data.address;
          this.cdr.markForCheck();
          this.noAccess = Boolean(data.error);
        },
        () => {
          this.isLoading = false;
          this.noAccess = true;
          this.cdr.markForCheck();
        }
      );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
