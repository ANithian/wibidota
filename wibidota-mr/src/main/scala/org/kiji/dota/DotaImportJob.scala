package org.kiji.dota

import scala.util.parsing.json.JSON
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.MultipleTextLineFiles
import com.twitter.scalding.Tsv
import org.kiji.express.DSL._
import com.twitter.scalding.TextLine
import org.kiji.dota._
import com.codahale.jerkson.Json._
import cascading.tuple.Fields
import org.kiji.express.DSL._
import org.kiji.express.EntityId
import java.lang.Long
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import java.util.ArrayList
import scala.collection.JavaConverters._

class DotaImportJob(args: Args) extends Job(args) {
  
//val outFile: String = args("output")
val inFile: String = args("input")

/*
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
*/
  def extractTeam(jsonObj: Map[String,Any], extractDire: Boolean): Team = {
    val team_name: String = if(extractDire) { "dire"} else { "radiant" }
    
    val tower_status = jsonObj.getOrElse("tower_status_%s".format(team_name),null)
    val barracks_status = jsonObj.getOrElse("barracks_status_%s".format(team_name),null)
    val is_victor = jsonObj("radiant_win").asInstanceOf[Boolean]
    val result = new Team()
    result.setIsVictor(is_victor)
    if(barracks_status != null)
      result.setBarracksStatus(barracks_status.asInstanceOf[Integer].longValue())
    if(tower_status != null)
      result.setTowerStatus(tower_status.asInstanceOf[Integer].longValue())
    
    return result
  }

  def extractPlayer(jsonObj: Map[String,Any]): Player = {
    val goldSpent = jsonObj.getOrElse("gold_spent",null)
    val gold = jsonObj.getOrElse("gold",null)
    val deaths = jsonObj.getOrElse("deaths",null)
    val hero_damage = jsonObj.getOrElse("hero_damage",null)
    val last_hits = jsonObj.getOrElse("last_hits",null)
    val player_slot = jsonObj.getOrElse("player_slot",null)
    val denies = jsonObj.getOrElse("denies",null)
    val tower_damage = jsonObj.getOrElse("tower_damage",null)
    val hero_id = jsonObj.getOrElse("hero_id",null)
    val xp_per_min = jsonObj.getOrElse("xp_per_min",null)
    val account_id = jsonObj.getOrElse("account_id",null)
    val kills = jsonObj.getOrElse("kills",null)
    val leaver_status = jsonObj.getOrElse("leaver_status",null)
    val hero_healing = jsonObj.getOrElse("hero_healing",null)
    val assists = jsonObj.getOrElse("assists",null)
    val gold_per_min = jsonObj.getOrElse("gold_per_min",null)
    val level = jsonObj.getOrElse("level",null)
    val item_4 = jsonObj.getOrElse("item_4",null)
    val item_5 = jsonObj.getOrElse("item_5",null)
    val item_2 = jsonObj.getOrElse("item_2",null)
    val item_3 = jsonObj.getOrElse("item_3",null)
    val item_0 = jsonObj.getOrElse("item_0",null)
    val item_1 = jsonObj.getOrElse("item_1",null)
    
    val player = new Player()
    if(goldSpent != null)
      player.setGoldSpent(goldSpent.asInstanceOf[Integer].longValue())
    if(gold != null)
      player.setGold(gold.asInstanceOf[Integer].longValue())
    if(deaths != null)
      player.setDeaths(deaths.asInstanceOf[Integer].longValue())
    if(hero_damage != null)
      player.setHeroDamage(hero_damage.asInstanceOf[Integer].longValue())
    if(last_hits != null)
      player.setLastHits(last_hits.asInstanceOf[Integer].longValue())
    if(player_slot != null)
      player.setPlayerSlot(player_slot.asInstanceOf[Integer].longValue())
    if(denies != null)
      player.setDenies(denies.asInstanceOf[Integer].longValue())
    if(tower_damage != null)
      player.setTowerDamage(tower_damage.asInstanceOf[Integer].longValue())
    if(hero_id != null)
      player.setHeroId(hero_id.asInstanceOf[Integer].longValue())
    if(xp_per_min != null)
      player.setXpPerMin(xp_per_min.asInstanceOf[Integer].longValue())
    if(account_id != null)
      player.setAccountId(account_id.asInstanceOf[Integer].longValue())
    if(kills != null)
      player.setKills(kills.asInstanceOf[Integer].longValue())
    if(leaver_status != null)
      player.setLeaverStatus(leaver_status.asInstanceOf[Integer].longValue())
    if(hero_healing != null)
      player.setHeroHealing(hero_healing.asInstanceOf[Integer].longValue())
    if(assists != null)
      player.setAssists(assists.asInstanceOf[Integer].longValue())
    if(gold_per_min != null)
      player.setGoldPerMin(gold_per_min.asInstanceOf[Integer].longValue())
    if(level != null)
      player.setLevel(level.asInstanceOf[Integer].longValue())
    if(item_4 != null)
      player.setItem4(item_4.asInstanceOf[Integer].longValue())
    if(item_5 != null)
      player.setItem5(item_5.asInstanceOf[Integer].longValue())
    if(item_2 != null)
      player.setItem2(item_2.asInstanceOf[Integer].longValue())
    if(item_3 != null)
      player.setItem3(item_3.asInstanceOf[Integer].longValue())
    if(item_0 != null)
      player.setItem0(item_0.asInstanceOf[Integer].longValue())
    if(item_1 != null)
      player.setItem1(item_1.asInstanceOf[Integer].longValue())
    
    return player
  }

/*
val matchFields = new Fields(
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
*/
val matchFields = (
    'entityId,
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
    'dire_team,
    'radiant_team,
    'players
    )
  val outFields = new HashMap[Symbol,String]()
  outFields += ('Hello -> "World")
  
  TextLine(inFile) 
    .map('line -> 'jsonLine) { line: String =>
      {
        val jsonParseResult = parse[Map[String,Any]](line)
        jsonParseResult
      }
    }
    .mapTo('jsonLine->matchFields) { jsonLine: Map[String,Any] => {
      val eid =EntityId("kiji://.env/dota2/matches")(jsonLine("match_seq_num"))
      val players = jsonLine("players")
      if(players != null)
      {
        players.asInstanceOf[ArrayList[Map[String,Any]]].asScala.map(p => {
          extractPlayer(p)
        })
      }
      
      (
          eid, 
          jsonLine("match_seq_num").asInstanceOf[Integer].longValue,
          jsonLine("match_id").asInstanceOf[Integer].longValue,
          jsonLine("human_players").asInstanceOf[Integer].longValue,
          jsonLine("cluster").asInstanceOf[Integer].longValue,
          jsonLine("season").asInstanceOf[Integer].longValue,
          jsonLine("start_time").asInstanceOf[Integer].longValue,
          jsonLine("game_mode").asInstanceOf[Integer].longValue,
          jsonLine("leagueid").asInstanceOf[Integer].longValue,
          jsonLine("first_blood_time").asInstanceOf[Integer].longValue,
          jsonLine("positive_votes").asInstanceOf[Integer].longValue,
          jsonLine("negative_votes").asInstanceOf[Integer].longValue,
          jsonLine("duration").asInstanceOf[Integer].longValue,
          jsonLine("lobby_type").asInstanceOf[Integer].longValue,
          extractTeam(jsonLine, true),
          extractTeam(jsonLine, false)
       )
     }
    }
    .write(KijiOutput("kiji://.env/dota2/matches")
          (
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
          'dire_team -> "match:dire_team",
          'radiant_team -> "match:radiant_team"
          )
        )
        /*
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
        'tower_status_radiant -> "match:tower_status_radiant"
        */
}