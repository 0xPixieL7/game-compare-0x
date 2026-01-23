import compare from './compare'
import games from './games'

const api = {
    compare: Object.assign(compare, compare),
    games: Object.assign(games, games),
}

export default api