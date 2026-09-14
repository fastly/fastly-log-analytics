import { describe, expect, it } from 'vitest';
import { CRON_EXPLANATIONS, CRON_DISPLAY_NAMES, CRON_GROUPS } from '../CronExplanations';

describe('partial_hour_merge cron registry entry', () => {
  it('has a label', () => {
    expect(CRON_DISPLAY_NAMES.partial_hour_merge).toBeTruthy();
  });

  it('has a description', () => {
    expect(CRON_EXPLANATIONS.partial_hour_merge).toBeTruthy();
  });

  it('is grouped alongside the other rollup jobs', () => {
    const rollupGroup = CRON_GROUPS.find((g) => g.tasks.includes('rollup_hour_heal'));
    expect(rollupGroup?.tasks).toContain('partial_hour_merge');
  });
});
