// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

import Vuex from 'vuex';

import { makeNodeModule, NODE_ACTIONS, NODE_MUTATIONS, StatusOnline } from '@/app/store/modules/node';
import { SNOApi } from '@/storagenode/api/storagenode';
import {
    BandwidthInfo,
    Dashboard,
    DiskSpaceInfo,
    SatelliteInfo,
} from '@/storagenode/dashboard';
import {
    BandwidthUsed,
    Egress,
    EgressUsed,
    Ingress,
    IngressUsed,
    Metric,
    Satellite,
    Satellites,
    Stamp,
} from '@/storagenode/satellite';
import { createLocalVue } from '@vue/test-utils';

const Vue = createLocalVue();

const nodeApi = new SNOApi();
const nodeModule = makeNodeModule(nodeApi);

Vue.use(Vuex);

const store = new Vuex.Store({ modules: { node: nodeModule } });

const state = store.state as any;

describe('mutations', () => {
    beforeEach(() => {
        createLocalVue().use(Vuex);
    });

    it('set dashboard info', () => {
        const dashboardInfo = new Dashboard(
            '1',
            '2',
            [
                new SatelliteInfo('3', 'url1', null, null),
                new SatelliteInfo('4', 'url2', new Date(2020, 1, 1), new Date(2020, 0, 1)),
            ],
            new DiskSpaceInfo(99, 100, 5),
            new BandwidthInfo(50),
            new Date(),
            new Date(2019, 3, 1),
            '0.1.1',
            '0.2.2',
            false,
        );

        store.commit(NODE_MUTATIONS.POPULATE_STORE, dashboardInfo);

        expect(state.node.info.id).toBe(dashboardInfo.nodeID);
        expect(state.node.utilization.bandwidth.used).toBe(dashboardInfo.bandwidth.used);
        expect(state.node.utilization.diskSpace.used).toBe(dashboardInfo.diskSpace.used);
        expect(state.node.utilization.diskSpace.trash).toBe(dashboardInfo.diskSpace.trash);
        expect(state.node.satellites.length).toBe(dashboardInfo.satellites.length);
        expect(state.node.disqualifiedSatellites.length).toBe(1);
        expect(state.node.suspendedSatellites.length).toBe(1);
        expect(state.node.info.status).toBe(StatusOnline);
    });

    it('selects single satellite', () => {
        const satelliteInfo = new Satellite(
            '3',
           [new Stamp()],
            [],
            [],
            [],
            111,
            222,
            50,
            70,
            new Metric(1, 1, 1, 0, 1),
            new Metric(2, 1, 1, 0, 1),
            new Date(2019, 3, 1),
        );

        store.commit(NODE_MUTATIONS.SELECT_SATELLITE, satelliteInfo);

        expect(state.node.selectedSatellite.id).toBe(satelliteInfo.id);
        expect(state.node.checks.audit).toBe(100);
        expect(state.node.checks.uptime).toBe(50);
    });

    it('don`t selects wrong satellite', () => {
        const satelliteInfo = new Satellite();

        store.commit(NODE_MUTATIONS.SELECT_SATELLITE, satelliteInfo);

        expect(state.node.selectedSatellite.id).toBe('3');
    });

    it('selects all satellites', () => {
        const satelliteInfo = new Satellites();

        store.commit(NODE_MUTATIONS.SELECT_ALL_SATELLITES, satelliteInfo);

        expect(state.node.selectedSatellite.id).toBe('');
    });

    it('sets daily data', () => {
        const satelliteInfo = new Satellite(
            '3',
            [new Stamp(), new Stamp()],
            [
                new BandwidthUsed(
                    new Egress(),
                    new Ingress(),
                    new Date(),
                ),
                new BandwidthUsed(
                    new Egress(),
                    new Ingress(),
                    new Date(),
                ),
            ],
            [
                new EgressUsed(new Egress(), new Date()),
                new EgressUsed(new Egress(), new Date()),
            ],
            [
                new IngressUsed(new Ingress(), new Date()),
                new IngressUsed(new Ingress(), new Date()),
            ],
            111,
            222,
            50,
            70,
            new Metric(1, 1, 1, 0, 1),
            new Metric(2, 1, 1, 0, 1),
            new Date(2019, 3, 1),
        );

        store.commit(NODE_MUTATIONS.SET_DAILY_DATA, satelliteInfo);

        expect(state.node.bandwidthChartData.length).toBe(2);
        expect(state.node.egressChartData.length).toBe(2);
        expect(state.node.ingressChartData.length).toBe(2);
        expect(state.node.storageChartData.length).toBe(2);
        expect(state.node.bandwidthSummary).toBe(satelliteInfo.bandwidthSummary);
        expect(state.node.egressSummary).toBe(satelliteInfo.egressSummary);
        expect(state.node.ingressSummary).toBe(satelliteInfo.ingressSummary);
        expect(state.node.storageSummary).toBe(satelliteInfo.storageSummary);
    });
});

describe('actions', () => {
    beforeEach(() => {
        jest.resetAllMocks();
    });

    it('throws error on failed node info fetch', async () => {
        jest.spyOn(nodeApi, 'dashboard').mockImplementation(() => { throw new Error(); });

        try {
            await store.dispatch(NODE_ACTIONS.GET_NODE_INFO);
            expect(true).toBe(false);
        } catch (error) {
            expect(state.node.info.id).toBe('1');
        }
    });

    it('success get node info', async () => {
        jest.spyOn(nodeApi, 'dashboard').mockReturnValue(
            Promise.resolve(
                new Dashboard(
                    '1',
                    '2',
                    [
                        new SatelliteInfo('3', 'url1', null, null),
                        new SatelliteInfo('4', 'url2', new Date(2020, 1, 1), new Date(2020, 0, 1)),
                    ],
                    new DiskSpaceInfo(99, 100, 1),
                    new BandwidthInfo(50),
                    new Date(),
                    new Date(2019, 3, 1),
                    '0.1.1',
                    '0.2.2',
                    false,
                ),
            ),
        );

        await store.dispatch(NODE_ACTIONS.GET_NODE_INFO);

        expect(state.node.info.id).toBe('1');
        expect(state.node.utilization.bandwidth.used).toBe(50);
        expect(state.node.utilization.diskSpace.used).toBe(99);
        expect(state.node.utilization.diskSpace.trash).toBe(1);
        expect(state.node.satellites.length).toBe(2);
        expect(state.node.disqualifiedSatellites.length).toBe(1);
        expect(state.node.suspendedSatellites.length).toBe(1);
        expect(state.node.info.status).toBe(StatusOnline);
    });

    it('fetch satellite info throws error on api call fail', async () => {
        jest.spyOn(nodeApi, 'satellite').mockImplementation(() => { throw new Error(); });

        try {
            await store.dispatch(NODE_ACTIONS.SELECT_SATELLITE, '3');
            expect(true).toBe(false);
        } catch (e) {
            expect(state.node.selectedSatellite.id).toBe('');
        }
    });

    it('success fetch single satellite info', async () => {
        jest.spyOn(nodeApi, 'satellite').mockReturnValue(
            Promise.resolve(
                new Satellite(
                    '4',
                    [new Stamp(), new Stamp()],
                    [
                        new BandwidthUsed(
                            new Egress(),
                            new Ingress(),
                            new Date(),
                        ),
                    ],
                    [
                        new EgressUsed(new Egress(), new Date()),
                    ],
                    [
                        new IngressUsed(new Ingress(), new Date()),
                    ],
                    1111,
                    2221,
                    501,
                    701,
                    new Metric(1, 1, 1, 0, 1),
                    new Metric(2, 1, 1, 0, 1),
                    new Date(2019, 3, 1),
                ),
            ),
        );

        await store.dispatch(NODE_ACTIONS.SELECT_SATELLITE, '4');

        expect(state.node.selectedSatellite.id).toBe('4');
        expect(state.node.bandwidthChartData.length).toBe(1);
        expect(state.node.egressChartData.length).toBe(1);
        expect(state.node.ingressChartData.length).toBe(1);
        expect(state.node.storageChartData.length).toBe(2);
        expect(state.node.bandwidthSummary).toBe(2221);
        expect(state.node.egressSummary).toBe(501);
        expect(state.node.ingressSummary).toBe(701);
        expect(state.node.storageSummary).toBe(1111);
    });

    it('fetch all satellites info throws error on api call fail', async () => {
        jest.spyOn(nodeApi, 'satellites').mockImplementation(
            () => { throw new Error(); },
        );

        try {
            await store.dispatch(NODE_ACTIONS.SELECT_SATELLITE);
            expect(true).toBe(false);
        } catch (e) {
            expect(state.node.selectedSatellite.id).toBe('4');
        }
    });

    it('success fetch all satellites info', async () => {
        jest.spyOn(nodeApi, 'satellites').mockReturnValue(
            Promise.resolve(new Satellites()),
        );

        await store.dispatch(NODE_ACTIONS.SELECT_SATELLITE);

        expect(state.node.selectedSatellite.id).toBe('');
    });
});

describe('getters', () => {
    it('getter monthsOnNetwork returns correct value',  () => {
        const dashboardInfo = new Dashboard(
            '1',
            '2',
            [
                new SatelliteInfo('3', 'url1', null, null),
                new SatelliteInfo('4', 'url2', new Date(2020, 1, 1), new Date(2020, 0, 1)),
            ],
            new DiskSpaceInfo(99, 100, 4),
            new BandwidthInfo(50),
            new Date(),
            new Date(2019, 3, 1),
            '0.1.1',
            '0.2.2',
            false,
        );
        const now = new Date();
        const testJoinAt = new Date(now.getTime() - 1000000);

        const satelliteInfo = new Satellite(
            '3',
            [new Stamp()],
            [],
            [],
            [],
            111,
            222,
            50,
            70,
            new Metric(1, 1, 1, 0, 1),
            new Metric(2, 1, 1, 0, 1),
            testJoinAt,
        );

        store.commit(NODE_MUTATIONS.POPULATE_STORE, dashboardInfo);
        store.commit(NODE_MUTATIONS.SELECT_SATELLITE, satelliteInfo);

        expect(store.getters.monthsOnNetwork).toBe(1);

        satelliteInfo.joinDate = new Date(testJoinAt.getTime() - 9e9);

        store.commit(NODE_MUTATIONS.SELECT_SATELLITE, satelliteInfo);

        expect(store.getters.monthsOnNetwork).toBe(4);
    });
});
