/*
 *  Copyright 2022 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { DateRangeObject } from 'Models';
import { ReactNode } from 'react';
import { OperationPermission } from '../../../../context/PermissionProvider/PermissionProvider.interface';
import { SystemProfile } from '../../../../generated/api/data/createTableProfile';
import {
  Column,
  ColumnProfilerConfig,
  PartitionProfilerConfig,
  ProfileSampleType,
  TableProfile,
  TableProfilerConfig,
} from '../../../../generated/entity/data/table';
import { Container } from '../../../../generated/entity/data/container';
import { TestCase, TestSummary } from '../../../../generated/tests/testCase';
import { UsePagingInterface } from '../../../../hooks/paging/usePaging';
import { ListTestCaseParams } from '../../../../rest/testAPI';

export interface TableProfilerProps {
  permissions: OperationPermission;
  container?: Container;
  testCaseSummary?: TestSummary;
}

export interface TableProfilerProviderProps extends TableProfilerProps {
  children: ReactNode;
}

export interface TableProfilerContextInterface {
  isDeleted?: boolean;
  testCaseSummary?: TestSummary;
  permissions: OperationPermission;
  isTestsLoading: boolean;
  isProfilerDataLoading: boolean;
  tableProfiler?: Container;
  customMetric?: Container;
  allTestCases: TestCase[];
  overallSummary: OverallTableSummaryType[];
  onTestCaseUpdate: (testCase?: TestCase) => void;
  onSettingButtonClick: () => void;
  fetchAllTests: (params?: ListTestCaseParams) => Promise<void>;
  onCustomMetricUpdate: (container: Container) => void;
  isProfilingEnabled: boolean;
  dateRangeObject: DateRangeObject;
  onDateRangeChange: (dateRange: DateRangeObject) => void;
  testCasePaging: UsePagingInterface;
  container?: Container;
}

export type TableTestsType = {
  tests: TestCase[];
  results: {
    success: number;
    aborted: number;
    failed: number;
  };
};

export type ModifiedColumn = Column & {
  testCount?: number;
};

export type columnTestResultType = {
  [key: string]: { results: TableTestsType['results']; count: number };
};

export interface ProfilerProgressWidgetProps {
  value: number;
  strokeColor?: string;
}

export interface ProfilerSettingsModalProps {
  containerId: string;
  columns: Column[];
  visible: boolean;
  onVisibilityChange: (visible: boolean) => void;
}

export interface TestIndicatorProps {
  value: number | string;
  type: string;
}

export type OverallTableSummaryType = {
  title: string;
  value: number | string;
  className?: string;
};

export type TableProfilerData = {
  tableProfilerData: TableProfile[];
  systemProfilerData: SystemProfile[];
};

export type TableProfilerChartProps = {
  entityFqn?: string;
  showHeader?: boolean;
  containerDetails?: Container;
};

export interface ProfilerSettingModalState {
  data: TableProfilerConfig | undefined;
  sqlQuery: string;
  profileSample: number | undefined;
  sampleDataCount?: number;
  excludeCol: string[];
  includeCol: ColumnProfilerConfig[];
  enablePartition: boolean;
  partitionData: PartitionProfilerConfig | undefined;
  selectedProfileSampleType: ProfileSampleType | undefined;
}

export interface ProfilerForm extends PartitionProfilerConfig {
  profileSample: number | undefined;
  selectedProfileSampleType: ProfileSampleType | undefined;
  enablePartition: boolean;
  profileSampleType: ProfileSampleType | undefined;
  profileSamplePercentage: number;
  profileSampleRows: number | undefined;
  includeColumns: ColumnProfilerConfig[];
}
