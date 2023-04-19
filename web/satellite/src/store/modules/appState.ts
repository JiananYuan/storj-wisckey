// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

import { APP_STATE_ACTIONS } from '@/utils/constants/actionNames';
import { AppState } from '@/utils/constants/appStateEnum';

import { APP_STATE_MUTATIONS } from '../mutationConstants';

export const appStateModule = {
    state: {
        // Object that contains all states of views
        appState: {
            fetchState: AppState.LOADING,
            isAddTeamMembersPopupShown: false,
            isNewProjectPopupShown: false,
            isProjectsDropdownShown: false,
            isAccountDropdownShown: false,
            isDeleteProjectPopupShown: false,
            isDeleteAccountPopupShown: false,
            isSortProjectMembersByPopupShown: false,
            isSuccessfulRegistrationShown: false,
            isSuccessfulProjectCreationPopupShown: false,
            isEditProfilePopupShown: false,
            isChangePasswordPopupShown: false,
            isPaymentSelectionShown: false,
            isCreateProjectButtonShown: false,
        },
        satelliteName: '',
    },
    mutations: {
        // Mutation changing add projectMembers members popup visibility
        [APP_STATE_MUTATIONS.TOGGLE_ADD_TEAMMEMBER_POPUP](state: any): void {
            state.appState.isAddTeamMembersPopupShown = !state.appState.isAddTeamMembersPopupShown;
        },

        // Mutation changing new project popup visibility
        [APP_STATE_MUTATIONS.TOGGLE_NEW_PROJECT_POPUP](state: any): void {
            state.appState.isNewProjectPopupShown = !state.appState.isNewProjectPopupShown;
        },

        // Mutation changing project dropdown visibility
        [APP_STATE_MUTATIONS.TOGGLE_PROJECT_DROPDOWN](state: any): void {
            state.appState.isProjectsDropdownShown = !state.appState.isProjectsDropdownShown;
        },

        // Mutation changing account dropdown visibility
        [APP_STATE_MUTATIONS.TOGGLE_ACCOUNT_DROPDOWN](state: any): void {
            state.appState.isAccountDropdownShown = !state.appState.isAccountDropdownShown;
        },

        // Mutation changing delete project popup visibility
        [APP_STATE_MUTATIONS.TOGGLE_DELETE_PROJECT_DROPDOWN](state: any): void {
            state.appState.isDeleteProjectPopupShown = !state.appState.isDeleteProjectPopupShown;
        },
        // Mutation changing delete account popup visibility
        [APP_STATE_MUTATIONS.TOGGLE_DELETE_ACCOUNT_DROPDOWN](state: any): void {
            state.appState.isDeleteAccountPopupShown = !state.appState.isDeleteAccountPopupShown;
        },
        // Mutation changing 'sort project members by' popup visibility.
        [APP_STATE_MUTATIONS.TOGGLE_SORT_PM_BY_DROPDOWN](state: any): void {
            state.appState.isSortProjectMembersByPopupShown = !state.appState.isSortProjectMembersByPopupShown;
        },
        // Mutation changing 'successful registration' area visibility.
        [APP_STATE_MUTATIONS.TOGGLE_SUCCESSFUL_REGISTRATION](state: any): void {
            state.appState.isSuccessfulRegistrationShown = !state.appState.isSuccessfulRegistrationShown;
        },
        // Mutation changing 'successful project creation' popup visibility.
        [APP_STATE_MUTATIONS.TOGGLE_SUCCESSFUL_PROJECT_CREATION_POPUP](state: any): void {
            state.appState.isSuccessfulProjectCreationPopupShown = !state.appState.isSuccessfulProjectCreationPopupShown;
        },
        [APP_STATE_MUTATIONS.TOGGLE_CHANGE_PASSWORD_POPUP](state: any): void {
            state.appState.isChangePasswordPopupShown = !state.appState.isChangePasswordPopupShown;
        },
        [APP_STATE_MUTATIONS.TOGGLE_EDIT_PROFILE_POPUP](state: any): void {
            state.appState.isEditProfilePopupShown = !state.appState.isEditProfilePopupShown;
        },
        [APP_STATE_MUTATIONS.SHOW_SET_DEFAULT_PAYMENT_METHOD_POPUP](state: any, id: string): void {
            state.appState.setDefaultPaymentMethodID = id;
        },
        [APP_STATE_MUTATIONS.SHOW_DELETE_PAYMENT_METHOD_POPUP](state: any, id: string): void {
            state.appState.deletePaymentMethodID = id;
        },
        [APP_STATE_MUTATIONS.SHOW_CREATE_PROJECT_BUTTON](state: any): void {
            state.appState.isCreateProjectButtonShown = true;
        },
        [APP_STATE_MUTATIONS.HIDE_CREATE_PROJECT_BUTTON](state: any): void {
            state.appState.isCreateProjectButtonShown = false;
        },
        // Mutation that closes each popup/dropdown
        [APP_STATE_MUTATIONS.CLOSE_ALL](state: any): void {
            state.appState.isAccountDropdownShown = false;
            state.appState.isProjectsDropdownShown = false;
            state.appState.isSortProjectMembersByPopupShown = false;
        },
        [APP_STATE_MUTATIONS.CHANGE_STATE](state: any, newFetchState: AppState): void {
            state.appState.fetchState = newFetchState;
        },

        // Mutation changing payment selection visibility
        [APP_STATE_MUTATIONS.TOGGLE_PAYMENT_SELECTION](state: any, value: boolean): void {
            state.appState.isPaymentSelectionShown = value;
        },
        [APP_STATE_MUTATIONS.SET_NAME](state: any, satelliteName: string): void {
            state.satelliteName = satelliteName;
        },
    },
    actions: {
        // Commits mutation for changing app popups and dropdowns visibility state
        [APP_STATE_ACTIONS.TOGGLE_TEAM_MEMBERS]: function ({commit, state}: any): void {
            if (!state.appState.isAddTeamMembersPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_ADD_TEAMMEMBER_POPUP);
        },
        [APP_STATE_ACTIONS.TOGGLE_NEW_PROJ]: function ({commit, state}: any): void {
            if (!state.appState.isNewProjectPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_NEW_PROJECT_POPUP);
        },
        [APP_STATE_ACTIONS.TOGGLE_PROJECTS]: function ({commit, state}: any): void {
            if (!state.appState.isProjectsDropdownShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_PROJECT_DROPDOWN);
        },
        [APP_STATE_ACTIONS.TOGGLE_ACCOUNT]: function ({commit, state}: any): void {
            if (!state.appState.isAccountDropdownShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_ACCOUNT_DROPDOWN);
        },
        [APP_STATE_ACTIONS.TOGGLE_DEL_PROJ]: function ({commit, state}: any): void {
            if (!state.appState.isDeleteProjectPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_DELETE_PROJECT_DROPDOWN);
        },
        [APP_STATE_ACTIONS.TOGGLE_DEL_ACCOUNT]: function ({commit, state}: any): void {
            if (!state.appState.isDeleteAccountPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_DELETE_ACCOUNT_DROPDOWN);
        },
        [APP_STATE_ACTIONS.TOGGLE_SORT_PM_BY_DROPDOWN]: function ({commit, state}: any): void {
            if (!state.appState.isSortProjectMembersByPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_SORT_PM_BY_DROPDOWN);
        },
        [APP_STATE_ACTIONS.TOGGLE_SUCCESSFUL_REGISTRATION]: function ({commit, state}: any): void {
            if (!state.appState.isSuccessfulRegistrationShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_SUCCESSFUL_REGISTRATION);
        },
        [APP_STATE_ACTIONS.TOGGLE_SUCCESSFUL_PROJECT_CREATION_POPUP]: function ({commit, state}: any): void {
            if (!state.appState.isSuccessfulProjectCreationPopupShown) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.TOGGLE_SUCCESSFUL_PROJECT_CREATION_POPUP);
        },
        [APP_STATE_ACTIONS.TOGGLE_CHANGE_PASSWORD_POPUP]: function ({commit}: any): void {
            commit(APP_STATE_MUTATIONS.TOGGLE_CHANGE_PASSWORD_POPUP);
        },
        [APP_STATE_ACTIONS.TOGGLE_EDIT_PROFILE_POPUP]: function ({commit}: any): void {
            commit(APP_STATE_MUTATIONS.TOGGLE_EDIT_PROFILE_POPUP);
        },
        [APP_STATE_ACTIONS.SHOW_SET_DEFAULT_PAYMENT_METHOD_POPUP]: function ({commit, state}: any, methodID: string): void {
            if (!state.appState.setDefaultPaymentMethodID) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.SHOW_SET_DEFAULT_PAYMENT_METHOD_POPUP, methodID);
        },
        [APP_STATE_ACTIONS.SHOW_DELETE_PAYMENT_METHOD_POPUP]: function ({commit, state}: any, methodID: string): void {
            if (!state.appState.deletePaymentMethodID) {
                commit(APP_STATE_MUTATIONS.CLOSE_ALL);
            }

            commit(APP_STATE_MUTATIONS.SHOW_DELETE_PAYMENT_METHOD_POPUP, methodID);
        },
        [APP_STATE_ACTIONS.SHOW_CREATE_PROJECT_BUTTON]: function ({commit}: any): void {
            commit(APP_STATE_MUTATIONS.SHOW_CREATE_PROJECT_BUTTON);
        },
        [APP_STATE_ACTIONS.HIDE_CREATE_PROJECT_BUTTON]: function ({commit}: any): void {
            commit(APP_STATE_MUTATIONS.HIDE_CREATE_PROJECT_BUTTON);
        },
        [APP_STATE_ACTIONS.CLOSE_POPUPS]: function ({commit}: any): void {
            commit(APP_STATE_MUTATIONS.CLOSE_ALL);
        },
        [APP_STATE_ACTIONS.CHANGE_STATE]: function ({commit}: any, newFetchState: AppState): void {
            commit(APP_STATE_MUTATIONS.CHANGE_STATE, newFetchState);
        },
        [APP_STATE_ACTIONS.TOGGLE_PAYMENT_SELECTION]: function ({commit, state}: any, value: boolean): void {
            commit(APP_STATE_MUTATIONS.TOGGLE_PAYMENT_SELECTION, value);
        },
        [APP_STATE_ACTIONS.SET_SATELLITE_NAME]: function ({commit}: any, satelliteName: string): void {
            commit(APP_STATE_MUTATIONS.SET_NAME, satelliteName);
        },
    },
};
