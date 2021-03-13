import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { SubmitSchedulerComponent } from './submit-scheduler.component';

const routes: Routes = [
  {
    path: '',
    component: SubmitSchedulerComponent
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class SubmitSchedulerRoutingModule {}
