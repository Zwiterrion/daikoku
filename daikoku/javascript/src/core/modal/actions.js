import { CLOSE_MODAL, OPEN_MODAL } from './';

export const openCreationTeamModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'teamCreation',
  });
};

export const openTeamSelectorModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'teamSelector',
  });
};

export const openAssetSelectorModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'assetSelector',
  });
};

export const openWysywygModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'wysywygModal',
  });
};

export const openSaveOrCancelModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'saveOrCancelModal',
  });
};

export const openSubMetadataModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'subscriptionMetadataModal',
  });
};

export const openContactModal = (
  name = undefined,
  email = undefined,
  tenant,
  team = undefined,
  api = undefined
) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps: { name, email, tenant, team, api },
    modalType: 'contactModal',
  });
};

export const openTestingApiKeyModal = (modalProps) => (dispatch) => {
  return dispatch({
    type: OPEN_MODAL,
    modalProps,
    modalType: 'testingApiKey',
  });
};

export const closeModal = () => (dispatch) => {
  return dispatch({
    type: CLOSE_MODAL,
  });
};
