import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule } from '@angular/forms';
import { ShareModule } from 'share/share.module';

import { SubmitSchedulerRoutingModule } from './submit-scheduler-routing.module';
import { SubmitSchedulerComponent } from './submit-scheduler.component';

@NgModule({
  imports: [CommonModule, ReactiveFormsModule, SubmitSchedulerRoutingModule, ShareModule],
  declarations: [SubmitSchedulerComponent]
})
export class SubmitSchedulerModule {}
