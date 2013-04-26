import scala.util.parsing.json.JSON
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.MultipleTextLineFiles
import com.twitter.scalding.Tsv
import org.kiji.express.DSL._
import com.twitter.scalding.TextLine
import org.kiji.dota._

class DotaImportJob(args: Args) extends Job(args) {
  
val outFile: String = args("output")
val inFile: String = args("input")

def getAbilityUpgrades(playerRecord: Map[String, Any]): List[AbilityUpgrade] = {
  playerRecord
      .get("ability_upgrades")
      .foreach {
        case abilityUpgrades: List[Map[String, Any]]): List[AbilityUpgrade] = {
          // TODO: Create an AbilityUpgrade builder.

          // TODO: Populate the builder.
          players.getOrElse("level", null)
          // TODO: ...

          // TODO: Build the AbilityUpgrade and emit it.
        }
        case _ => null
      }
}

def getAdditionalUnits(playerRecord: Map[String, Any]): List[AdditionalUnit] = {
  playerRecord
      .get("additional_units")
      .foreach {
        case additionalUnits: List[Map[String, Any]] => {
          // TODO: Create a AdditionalUnit builder.

          // TODO: Populate the builder.
          players.getOrElse("unitname", null)
          // TODO: ...

          // TODO: Build the AdditionalUnit and emit it.
        }
        case _ => null
      }
}

def getPlayers(matchRecord: Map[String, Any]): List[Player] = {
  matchRecord
      .get("players")
      .foreach {
        case players: List[Map[String, Any]] => {
          players.map { player: Map[String, Any] =>
            // TODO: Create a Player builder.

            // TODO: Populate the builder.
            players.getOrElse("gold_spent", null)
            // TODO: ...

            // TODO: Build the Player and emit it.
          }
        }
        case _ => List()
      }
}

def getPicksBans(matchRecord: Map[String, Any]): List[PickBan] = {
  matchRecord
      .get("picks_bans")
      .foreach {
        case picksBans: List[Map[String, Any]] => {
          picksBans.map { pickBan: Map[String, Any] =>
            // TODO: Create a PickBan builder.

            // TODO: Populate the builder.
            pickBan.getOrElse("team", null)
            // TODO: ...

            // TODO: Build the PickBan and emit it.
          }
        }
        case _ => List()
      }
}

def getHeros(str: String): List[Int] = {
  JSON.parseFull(str) match {
    case Some(m: Map[String, Any]) => m("players") match {
      case pMap: List[Map[String, Double]] => pMap.map(_("hero_id").toInt)
    }
  }
}

val matchFields = (
    'match_seq_num,
    'match_id,
    'human_players,
    'cluster,
    'season,
    'start_time,
    'game_mode,
    'leagueid,
    'first_blood_time,
    'positive_votes,
    'negative_votes,
    'duration,
    'lobby_type,
    'radiant_win,
    'dire_team_id,
    'dire_team_complete,
    'dire_name,
    'dire_logo,
    'tower_status_dire,
    'barracks_status_dire,
    'radiant_team_id,
    'radiant_name,
    'radiant_team_complete,
    'radiant_logo,
    'barracks_status_radiant,
    'tower_status_radiant)

TextLine(inFile)
    .flatMap('line -> matchFields) { line: String =>
      JSON
          .parseFull(line)
          .map {
            case matchRecord: Map[String, Any] => {
              // TODO: Break up the record into its constituent parts.
              
            }
          }
    }
    .write(KijiOutput("kiji://.env/dota2/matches")(
        'match_seq_num -> "match:match_seq_num",
        'match_id -> "match:match_id",
        'human_players -> "match:human_players",
        'cluster -> "match:cluster",
        'season -> "match:season",
        'start_time -> "match:start_time",
        'game_mode -> "match:game_mode",
        'leagueid -> "match:leagueid",
        'first_blood_time -> "match:first_blood_time",
        'positive_votes -> "match:positive_votes",
        'negative_votes -> "match:negative_votes",
        'duration -> "match:duration",
        'lobby_type -> "match:lobby_type",
        'radiant_win -> "match:radiant_win",
        'dire_team_id -> "match:dire_team_id",
        'dire_team_complete -> "match:dire_team_complete",
        'dire_name -> "match:dire_name",
        'dire_logo -> "match:dire_logo",
        'tower_status_dire -> "match:tower_status_dire",
        'barracks_status_dire -> "match:barracks_status_dire",
        'radiant_team_id -> "match:radiant_team_id",
        'radiant_name -> "match:radiant_name",
        'radiant_team_complete -> "match:radiant_team_complete",
        'radiant_logo -> "match:radiant_logo",
        'barracks_status_radiant -> "match:barracks_status_radiant",
        'tower_status_radiant -> "match:tower_status_radiant"))

}