import IgdbWebhookController from './IgdbWebhookController'
import LandingController from './LandingController'
import DashboardController from './DashboardController'
import CompareController from './CompareController'
import VideoGameController from './VideoGameController'
import AIAssistantController from './AIAssistantController'
import Settings from './Settings'

const Controllers = {
    IgdbWebhookController: Object.assign(IgdbWebhookController, IgdbWebhookController),
    LandingController: Object.assign(LandingController, LandingController),
    DashboardController: Object.assign(DashboardController, DashboardController),
    CompareController: Object.assign(CompareController, CompareController),
    VideoGameController: Object.assign(VideoGameController, VideoGameController),
    AIAssistantController: Object.assign(AIAssistantController, AIAssistantController),
    Settings: Object.assign(Settings, Settings),
}

export default Controllers