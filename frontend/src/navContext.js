import { shallowRef } from 'vue'

const NAV_SIDEBAR = 'sidebar'
const NAV_INTERNAL = 'internal'

let nextNavigationSource = null
const lastTransition = shallowRef(null)

export function markNextNavigationFromSidebar() {
  nextNavigationSource = NAV_SIDEBAR
}

export function consumeNextNavigationSource() {
  const source = nextNavigationSource || NAV_INTERNAL
  nextNavigationSource = null
  return source
}

export function setLastTransition(transition) {
  lastTransition.value = transition
}

export function getLastTransition() {
  return lastTransition.value
}

export function getLastTransitionRef() {
  return lastTransition
}

export const NavSource = {
  Sidebar: NAV_SIDEBAR,
  Internal: NAV_INTERNAL,
}
